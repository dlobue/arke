
from Queue import Empty
from time import time
import logging
import shelve

import simpledaemon
from yapsy.PluginManager import PluginManager
from eventlet import Queue, GreenPool, sleep

import config
import persist


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
        plugin_manager = PluginManager(directories_list=plugin_dirs)
        plugin_manager.collectPlugins()

        no_plugins_activated = True
        for plugin_info in plugin_manager.getAllPlugins():
            if not (self.config_parser.has_option(plugin_info.name, 'enabled') and
                self.config_parser.getboolean(plugin_info.name, 'enabled')):
                logging.debug(("discovered plugin %s is not enabled. "
                               "skipping activation") % plugin_info.name)
                continue

            logging.debug("activating plugin %s" % plugin_info.name)
            plugin_manager.activatePluginByName(plugin_info.name)
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
                action,item = self.run_queue.get(True, 30)
            except Empty:
                logging.debug("didn't find squat. sleeping for a bit before trying again.")
                sleep(1)
                continue

            logging.debug("got something! action >%s< item >%r<" % (action, item))
            pool.spawn_n(getattr(self, action), item, spool, persist_backend)


    def gather_data(self, plugin, spool, persist_backend):
        extra = {}
        timestamp = time()
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

        if plugin.serialize:
            extra['ctype'] = plugin.serialize.lower()

        sourcetype = plugin.name
        logging.debug("gathering data for %s sourcetype" % sourcetype)
        data = plugin()

        key = '%s%s' % (timestamp, sourcetype)
        spool[key] = (sourcetype, timestamp, data, extra)
        
        self.run_queue.put(('persist_data', key))

    def persist_data(self, key, spool, persist_backend):
        (sourcetype, timestamp, data, extra) = spool[key]
        logging.debug("persisting data for %s sourcetype" % sourcetype)

        #XXX: queue for later, or do now?
        if persist_backend.write(sourcetype, timestamp, data, self.hostname, extra):
            spool.pop(key)
        else:
            self.run_queue.put(('persist_data', key))


if __name__ == '__main__':
    agent_daemon().main()

