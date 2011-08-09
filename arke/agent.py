
from Queue import Empty, Queue
from time import time
import logging
import shelve
import signal
import optparse

from gevent import monkey, spawn, sleep
from gevent.pool import Pool

#monkey.patch_all(httplib=True)

from circuits import Component, Event
from circuits.core.handlers import HandlerMetaClass
from circuits.app.config import Config as _Config

from circuits import Debugger

Config = HandlerMetaClass('Config', (_Config,), _Config.__dict__.copy())

from arke.collect import Collect
from arke.plugin import CollectPlugins
from arke.persist import Persist


RETRY_INTERVAL_CAP = 300
DEFAULT_CONFIG_FILE = '/etc/arke/arke.conf'


def argparser():
    p = optparse.OptionParser()
    p.add_option('-c', dest='config_filename',
                 action='store', default=DEFAULT_CONFIG_FILE,
                 help='Specify alternate configuration file name')
    p.add_option('-d', '--daemonize', dest='daemonize',
                 action='store_true', default=False,
                 help='Run in the foreground')
    options, args = p.parse_args()


class Agent(Component):
    section = 'agent'
    def __init__(self, config_file=DEFAULT_CONFIG_FILE):
        super(Agent, self).__init__()

        self.config = Config(config_file).register(self)
        self.config.load()

        self.collect_manager = CollectPlugins(base_class=Collect,
                                             entry_points='arke_plugins',
                                            ).register(self)
        self.persist_manager = Persist(1).register(self)


    def exception(self, *args, **kwargs):
        from pprint import pprint
        pprint(args)
        pprint(kwargs)
        raise SystemExit

    def started(self, *args):
        self.collect_manager.load()


if __name__ == '__main__':
    mylogger = logging.getLogger()
    mylogger.setLevel(logging.DEBUG)
    h = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    h.setFormatter(formatter)
    mylogger.addHandler(h)

    import sys

    (Agent(sys.argv[1]) + Debugger()).run()




class NoPlugins(Exception): pass

Daemon = object
class agent_daemon(Daemon):
    default_conf = '/etc/arke/arke.conf'
    section = 'agent'

    def read_basic_config(self):
        super(agent_daemon, self).read_basic_config()
        self.hostname = self.config_parser.get('core', 'hostname')
        config.set_main_object(self)

    def __init__(self):
        self.run_queue = Queue()
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

    def add_signal_handlers(self):
        super(self.__class__, self).add_signal_handlers()
        signal.signal(signal.SIGHUP, self.on_sighup)
        signal.signal(signal.SIGINT, self.on_sigterm)

    def run(self):
        logging.debug("initializing spool")
        self.spool = shelve.open(
            self.config_parser.get('core', 'spool_file'))

        self.load_plugins()
        gather_runner = spawn(self.gather_runner)
        persist_runner = spawn(self.persist_runner)

        while 1:
            if self.stop_now and gather_runner.dead and persist_runner.dead:
                break
            sleep(5)

        self.spool.close()



    def load_plugins(self):
        plugin_dirs = self.config_parser.get('core', 'plugin_dirs')
        plugin_dirs = plugin_dirs.replace(',', ' ')
        plugin_dirs = plugin_dirs.split()

        logging.debug("initializing plugin subsystem")
        self.plugin_manager = get_plugin_manager(plugin_dirs)

        no_plugins_activated = True
        for plugin in self.plugin_manager.collection_plugins:
            if not plugin.enabled:
                if plugin.is_activated:
                    logging.info(("active plugin %s is no longer enabled "
                                  "- deactivating.") % plugin.name)
                    plugin.deactivate()
                else:
                    logging.debug(("discovered plugin %s is not enabled. "
                               "skipping activation") % plugin.name)
                continue

            logging.info("activating plugin %s" % plugin.name)
            if not plugin.is_activated:
                plugin.activate()
            no_plugins_activated = False


        if no_plugins_activated:
            raise NoPlugins("No plugins found or enabled.")



    def gather_runner(self):
        pool = Pool(1000)

        while 1:
            if self.stop_now:
                break
            try:
                plugin = self.run_queue.get(True, 5)
            except Empty:
                sleep(1)
                continue

            pool.spawn(self.gather_data, plugin, self.spool)


    def persist_runner(self):
        logging.debug("initializing backend %s" % self.config_parser.get('core', 'persist_backend'))
        persist_backend = getattr(persist, '%s_backend' %
                self.config_parser.get('core', 'persist_backend'))
        persist_backend = persist_backend(config.get_config())

        if self.spool:
            logging.info("found data already in spool. adding to queue")
            for key in self.spool:
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


    def gather_data(self, plugin, spool):
        timestamp = time()
        sourcetype = plugin.name
        #key needs to be generated before we normalize
        key = '%f%s' % (timestamp, sourcetype)
        extra = {}

        if plugin.timestamp_as_id:
            #if the timestamp is going to be used as the id, then
            #that means we're going to group multiple results into
            #the same document. round the timestamp in order to ensure
            #we catch everything.
            offset = timestamp % plugin.get_setting('interval')
            timestamp = timestamp - offset
            extra['timestamp_as_id'] = True

        if plugin.custom_schema:
            extra['custom_schema'] = True

        if plugin.format:
            extra['ctype'] = plugin.format.lower()

        logging.debug("gathering data for %s sourcetype" % sourcetype)
        try:
            data = plugin()
        except Exception, e:
            logging.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
            raise e

        spool[key] = (sourcetype, timestamp, data, extra)
        
        self.persist_queue.put(key)


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

        spool.pop(key)


#if __name__ == '__main__':
    #agent_daemon().main()

