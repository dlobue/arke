
from Queue import Empty
from time import time
import logging
import shelve

import simpledaemon
from eventlet import Queue, GreenPool, sleep, spawn

import config
import persist
from plugin import get_plugin_manager


RETRY_INTERVAL_CAP = 300


class NoPlugins(Exception): pass

class agent_daemon(simpledaemon.Daemon):
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

    def on_sigterm(self, signalnum, frame):
        logging.info("got sigterm")
        self.stop_now = True

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
                logging.debug(("discovered plugin %s is not enabled. "
                               "skipping activation") % plugin.name)
                continue

            logging.info("activating plugin %s" % plugin.name)
            plugin.activate()
            no_plugins_activated = False


        if no_plugins_activated:
            raise NoPlugins("No plugins found or enabled.")



    def gather_runner(self):
        pool = GreenPool(100)

        while 1:
            if self.stop_now:
                break
            try:
                plugin = self.run_queue.get(True, 5)
            except Empty:
                sleep(1)
                continue

            pool.spawn_n(self.gather_data, plugin, self.spool)


    def persist_runner(self):
        logging.debug("initializing backend %s" % self.config_parser.get('core', 'persist_backend'))
        persist_backend = getattr(persist, '%s_backend' %
                self.config_parser.get('core', 'persist_backend'))
        persist_backend = persist_backend(config.get_config())

        if self.spool:
            logging.info("found data already in spool. adding to queue")
            for key in self.spool:
                self.persist_queue.put(('persist_data', key))

        pool = GreenPool(10)

        while 1:
            if self.stop_now:
                break
            try:
                item = self.persist_queue.get(True, 5)
            except Empty:
                sleep(1)
                continue

            pool.spawn_n(self.persist_data, item, self.spool, persist_backend)


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
        data = plugin()

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


if __name__ == '__main__':
    agent_daemon().main()

