
import logging
from Queue import Queue, Empty
from time import sleep

import simpledaemon
from pubsub import pub
from yapsy.PluginManager import PluginManager

import config


class agent_daemon(simpledaemon.Daemon):
    default_conf = '/etc/cagentd/cagentd.conf'
    section = 'agent'

    def read_basic_config(self):
        super(agent_daemon, self).read_basic_config()
        config.set_main_object(self)

    def __init__(self):
        self.run_queue = Queue()
        pub.subscribe(self.run_queue.put, "run_queue")

    def run(self):
        plugin_manager = PluginManager(directories_list=None)
        plugin_manager.collectPlugins()

        for plugin_info in plugin_manager.getAllPlugins():
            plugin_manager.activatePluginByName(plugin_info.name)

        persist_backend = _

        while 1:
            try:
                item = self.run_queue.get(True, 30)
            except Empty:
                sleep(1)
                continue

            persist_backend.write(item())


if __name__ == '__main__':
    agent_daemon().main()

