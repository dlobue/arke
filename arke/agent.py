
import gevent_fix
from gevent import monkey, sleep
monkey.patch_all(httplib=True, thread=False)

from Queue import Empty, LifoQueue
import ConfigParser
import optparse
import os
import logging
import signal

logger = logging.getLogger(__name__)

from gevent.pool import Pool



from arke.collect import Collect
from arke.plugin import CollectPlugins
from arke.spool import Spooler
from arke.plugins import persist
from arke.errors import PersistError


RETRY_INTERVAL_CAP = 300
DEFAULT_CONFIG_FILE = '/etc/arke/arke.conf'

GATHER_POOL_WORKERS = 1000
PERSIST_POOL_WORKERS = 10


class agent_daemon(object):
    default_conf = '/etc/arke/arke.conf'
    section = 'core'

    def read_basic_config(self):
        self.config_filename = self.options.config_filename
        cp = ConfigParser.ConfigParser()
        cp.read([self.config_filename])
        self.config_parser = cp
        self.hostname = self.config_parser.get('core', 'hostname')

    def __init__(self):
        self.persist_queue = LifoQueue()
        self.spool = None
        self.stop_now = False

    def on_sighup(self, signalnum, frame):
        logger.info("got sighup")
        self.config_parser.read(self.config_filename)
        self.collect_manager.load(pool=self._gather_pool)

    def on_sigterm(self, signalnum, frame):
        logger.info("got sigterm")
        self.stop_now = True

    def shutdown(self):
        [x.deactivate() for x in self.collect_manager._plugins if x._timer is not None]
        self._gather_pool.join()
        self.spool.close()

    def add_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.on_sigterm)
        signal.signal(signal.SIGHUP, self.on_sighup)
        signal.signal(signal.SIGINT, self.on_sigterm)




    def config_logging(self):
        if self.config_parser.has_option(self.section, 'logconfig'):
            import logging.config
            logging.config.fileConfig( self.config_parser.get(self.section, 'logconfig') )
        else:
            self._config_logging()

    def _config_logging(self):
        """Configure the logging module"""
        self.logfile = self.config_parser.get(self.section, 'logfile')
        loglevel = self.config_parser.get(self.section, 'loglevel')

        try:
            level = int(loglevel)
        except ValueError:
            level = int(logging.getLevelName(loglevel.upper()))


        handlers = []
        if self.config_parser.has_option(self.section, 'logfile'):
            logfile = self.config_parser.get(self.section, 'logfile').strip()

        if logfile:
            handlers.append(logging.FileHandler(logfile))
        else:
            handlers.append(logging.StreamHandler())

        log = logging.getLogger()
        log.setLevel(level)
        for h in handlers:
            h.setFormatter(logging.Formatter(
                "%(asctime)s %(process)d %(levelname)s %(message)s"))
            log.addHandler(h)


    def main(self):
        """Read the command line and either start or stop the daemon"""
        self.parse_options()
        self.read_basic_config()
        self.add_signal_handlers()
        self.config_logging()
        self.run()


    def parse_options(self):
        """Parse the command line"""
        p = optparse.OptionParser()
        p.add_option('-c', dest='config_filename',
                     action='store', default=self.default_conf,
                     help='Specify alternate configuration file name')
        self.options, self.args = p.parse_args()
        if not os.path.exists(self.options.config_filename):
            p.error(f'configuration file not found: {self.options.config_filename}')




    def run(self):
        logger.debug("initializing spool")
        config = self.config_parser

        self.spool = spool = Spooler(config)

        num_gather_workers = None
        if config.has_option('core', 'gather_workers'):
            num_gather_workers = abs(config.getint('core', 'gather_workers'))

        if not num_gather_workers:
            num_gather_workers = GATHER_POOL_WORKERS

        self._gather_pool = pool = Pool(num_gather_workers)

        persist_queue = self.persist_queue

        self.collect_manager = CollectPlugins(base_class=Collect,
                                              config=config,
                                              init_args=(config, persist_queue, spool, pool),
                                             entry_points='arke_plugins',
                                            )

        self.collect_manager.load(pool=self._gather_pool)
        try:
            self.persist_runner()
        except KeyboardInterrupt:
            pass

        self.shutdown()



    def persist_runner(self):
        config = self.config_parser
        logger.debug(f"initializing backend {config.get('core', 'persist_backend')}")
        persist_backend = getattr(
            persist, f"{config.get('core', 'persist_backend')}_backend"
        )


        persist_backend = persist_backend(config)

        spool = self.spool

        num_persist_workers = None
        if config.has_option('core', 'persist_workers'):
            num_persist_workers = abs(config.getint('core', 'persist_workers'))

        if not num_persist_workers:
            num_persist_workers = PERSIST_POOL_WORKERS

        self.persist_pool = pool = Pool(num_persist_workers)

        while 1 and not self.stop_now:
            pool.wait_available()
            spool_file = None
            try:
                spool_file = spool.get(5)
            except Empty:
                sleep(1)
                continue

            pool.spawn(self.persist_data, spool_file, persist_backend)

        pool.join()


    def persist_data(self, spool_file, persist_backend):
        if spool_file is None:
            logger.debug("Told to persist spool_file None!")
            return

        attempt = 1
        retry = .2
        while 1:
            if self.stop_now:
                return
            try:
                logger.debug("persisting data- spool_file: %s, attempt: %r" % (spool_file.name, attempt))
                persist_backend.batch_write(spool_file)
                break
            except PersistError:
                logger.warn(
                    f"attempt {attempt} trying to persist spool_file: {spool_file.name}"
                )

            except Exception:
                logger.exception(
                    f"attempt {attempt} trying to persist spool_file: {spool_file.name}"
                )


            sleep(retry)
            if retry < RETRY_INTERVAL_CAP:
                retry = attempt * 2
            attempt += 1

        self.spool.delete(spool_file)


if __name__ == '__main__':
    agent_daemon().main()

