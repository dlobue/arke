
from Queue import Empty, Queue
import logging
import signal

from simpledaemon import Daemon
from gevent import monkey, spawn, sleep
from gevent.pool import Pool

monkey.patch_all(httplib=True)


from arke.collect import Collect
from arke.plugin import CollectPlugins
from arke.spool import Spooler
from arke.plugins import persist


RETRY_INTERVAL_CAP = 300
DEFAULT_CONFIG_FILE = '/etc/arke/arke.conf'


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
        self.config_parser.read(self.default_conf)
        self.load_plugins()

    def on_sigterm(self, signalnum, frame):
        logging.info("got sigterm")
        self.stop_now = True
        self._gather_pool.kill()

    def add_signal_handlers(self):
        super(self.__class__, self).add_signal_handlers()
        signal.signal(signal.SIGHUP, self.on_sighup)
        signal.signal(signal.SIGINT, self.on_sigterm)

    def run(self):
        logging.debug("initializing spool")
        self.spool = spool = Spooler(self.config_parser).open()
        self._gather_pool = pool = Pool(1000)

        config = self.config_parser
        persist_queue = self.persist_queue

        self.collect_manager = CollectPlugins(base_class=Collect,
                                              config=config,
                                              init_args=(config, persist_queue, spool, pool),
                                             entry_points='arke_plugins',
                                            )


        self.collect_manager.load()
        persist_runner = spawn(self.persist_runner)

        while 1:
            if self.stop_now and persist_runner.dead:
                break
            sleep(5)

        self.spool.close()



    def persist_runner(self):
        logging.debug("initializing backend %s" % self.config_parser.get('core', 'persist_backend'))
        persist_backend = getattr(persist, '%s_backend' %
                self.config_parser.get('core', 'persist_backend'))

        persist_backend = persist_backend(self.config_parser)

        for key in self.spool.keys():
            self.persist_queue.put(key)

        pool = Pool(100)

        while 1:
            if self.stop_now:
                break
            try:
                item = self.persist_queue.get(True, 5)
            except Empty:
                sleep(1)
                continue

            pool.spawn(self.persist_data, item, self.spool, persist_backend)


    def persist_data(self, key, spool, persist_backend):
        (sourcetype, timestamp, data, extra) = spool[key]
        logging.debug("persisting data for %s sourcetype" % sourcetype)

        attempt = 1
        retry = .2
        while 1:
            try:
                persist_backend.write(sourcetype, timestamp, data, self.hostname, extra)
            except Exception:
                logging.exception("attempt %s trying to persist %s data. spool key: %s" % (attempt, sourcetype, key))
            else:
                break

            sleep(retry)
            if retry < RETRY_INTERVAL_CAP:
                retry = attempt * 2
            attempt += 1

        spool.delete(key)


#if __name__ == '__main__':
    #agent_daemon().main()

