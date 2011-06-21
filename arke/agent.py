
from Queue import Empty
from time import time
import logging
import shelve

import simpledaemon
from eventlet import Queue, GreenPool, sleep

import config
import persist
from plugin import get_plugin_manager


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
        self.spool = None
        self.stop_now = False

    def on_sigterm(self, signalnum, frame):
        logging.info("got sigterm")
        self.stop_now = True

    def run(self):
        logging.debug("initializing spool")
        if self.spool is None:
            self.spool = spool = shelve.open(
                self.config_parser.get('core', 'spool_file'))
        else:
            spool = self.spool

        logging.debug("initializing backend %s" % self.config_parser.get('core', 'persist_backend'))
        persist_backend = getattr(persist, '%s_backend' %
                self.config_parser.get('core', 'persist_backend'))
        persist_backend = persist_backend(config.get_config())

        if spool:
            logging.info("found data already in spool. adding to queue")
            for key in spool:
                self.persist_data(key, spool, persist_backend)

        plugin_dirs = self.config_parser.get('core', 'plugin_dirs')
        plugin_dirs = plugin_dirs.replace(',', ' ')
        plugin_dirs = plugin_dirs.split()

        logging.debug("initializing plugin subsystem")
        plugin_manager = get_plugin_manager(plugin_dirs)

        no_plugins_activated = True
        for plugin in plugin_manager.collection_plugins:
            if not plugin.enabled:
                logging.debug(("discovered plugin %s is not enabled. "
                               "skipping activation") % plugin.name)
                continue

            logging.info("activating plugin %s" % plugin.name)
            plugin.activate()
            no_plugins_activated = False


        if no_plugins_activated:
            raise NoPlugins("No plugins found or enabled.")

        pool = GreenPool(1000)

        while 1:
            if self.stop_now:
                spool.close()
                spool = None
                break

            logging.debug("going to get next item from queue")
            try:
                action,item = self.run_queue.get(True, 5)
            except Empty:
                logging.debug("didn't find squat. sleeping for a bit before trying again.")
                sleep(1)
                continue

            logging.debug("got something! action >%s< item >%r<" % (action, item))
            pool.spawn_n(getattr(self, action), item, spool, persist_backend)


    def gather_data(self, plugin, spool, persist_backend):
        timestamp = time()
        sourcetype = plugin.name
        #XXX: key needs to be generated before we normalize
        key = '%f%s' % (timestamp, sourcetype)
        extra = {}

        if plugin.timestamp_as_id:
            #if the timestamp is going to be used as the id, then
            #that means we're going to group multiple results into
            #the same document. round the timestamp in order to ensure
            #we catch everything.
            offset = timestamp % plugin.get_setting('interval')
            timestamp = timestamp - offset
            extra['_id'] = timestamp

        if plugin.custom_schema:
            extra['custom'] = True

        if plugin.format:
            extra['ctype'] = plugin.format.lower()

        logging.debug("gathering data for %s sourcetype" % sourcetype)
        data = plugin()

        spool[key] = (sourcetype, timestamp, data, extra)
        
        self.run_queue.put(('persist_data', key))

    def persist_data(self, key, spool, persist_backend):
        (sourcetype, timestamp, data, extra) = spool[key]
        logging.debug("persisting data for %s sourcetype" % sourcetype)

        try:
            persist_successful = persist_backend.write(sourcetype, timestamp, data, self.hostname, extra)
            persist_successful = True
        except Exception:
            logging.exception("Got an exception while trying to persist %s data. spool key: %s" % (sourcetype, key))
            persist_successful = False

        if persist_successful:
            spool.pop(key)
        else:
            self.run_queue.put(('persist_data', key))


if __name__ == '__main__':
    agent_daemon().main()

