
from gevent import monkey, spawn, sleep
monkey.patch_all(httplib=True, thread=False)

from Queue import Empty, Queue
import logging
import signal

logger = logging.getLogger(__name__)

from simpledaemon import Daemon
from gevent.pool import Pool



from arke.collect import Collect
from arke.plugin import CollectPlugins
from arke.spool import Spooler
from arke.plugins import persist


RETRY_INTERVAL_CAP = 300
DEFAULT_CONFIG_FILE = '/etc/arke/arke.conf'

GATHER_POOL_WORKERS = 1000
PERSIST_POOL_WORKERS = 10


class NoPlugins(Exception): pass

class agent_daemon(Daemon):
    default_conf = '/etc/arke/arke.conf'
    section = 'agent'

    def read_basic_config(self):
        super(agent_daemon, self).read_basic_config()
        self.hostname = self.config_parser.get('core', 'hostname')

    def __init__(self):
        self.persist_queue = Queue()
        self.spool = None
        self.stop_now = False

    def on_sighup(self, signalnum, frame):
        logging.info("got sighup")
        self.config_parser.read(self.config_filename)
        self.collect_manager.load(pool=self._gather_pool)

    def on_sigterm(self, signalnum, frame):
        logging.info("got sigterm")
        self.stop_now = True

    def shutdown(self):
        [x.deactivate() for x in self.collect_manager._plugins if x._timer is not None]
        self._gather_pool.join()
        self.spool.close()

    def add_signal_handlers(self):
        super(self.__class__, self).add_signal_handlers()
        signal.signal(signal.SIGHUP, self.on_sighup)
        signal.signal(signal.SIGINT, self.on_sigterm)

    def run(self):
        logging.debug("initializing spool")
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
        logging.debug("initializing backend %s" % config.get('core', 'persist_backend'))
        persist_backend = getattr(persist, '%s_backend' %
                config.get('core', 'persist_backend'))

        persist_backend = persist_backend(config)

        spool = self.spool

        num_persist_workers = None
        if config.has_option('core', 'persist_workers'):
            num_persist_workers = abs(config.getint('core', 'persist_workers'))

        if not num_persist_workers:
            num_persist_workers = PERSIST_POOL_WORKERS

        self.persist_pool = pool = Pool(num_persist_workers)

        while 1:
            spool_file = None
            if self.stop_now:
                break
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
                logging.debug("persisting data- spool_file: %s, attempt: %r" % (spool_file.name, attempt))
                persist_backend.batch_write(spool_file)
                break
            except Exception:
                logging.exception("attempt %s trying to persist spool_file: %s" % (attempt, spool_file.name))

            sleep(retry)
            if retry < RETRY_INTERVAL_CAP:
                retry = attempt * 2
            attempt += 1

        self.spool.delete(spool_file)


if __name__ == '__main__':
    agent_daemon().main()

