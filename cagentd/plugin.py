
from yapsy.IPlugin import IPlugin
import timer2
from pubsub import pub

import config

class collect_plugin(IPlugin):
    name = None
    default_config = {'interval': 30}

    @property
    def config(self):
        return config.get_config()

    def __init__(self):
        IPlugin.__init__(self)
        self._timer = None
        assert 'interval' in self.default_config, (
            "Missing default interval value for %s plugin" % self.name)

    def activate(self):
        super(collect_plugin, self).activate()

        if self.config.has_option(self.name, "interval"):
            secs = self.config.getint(self.name, "interval")
        else:
            secs = self.default_config['interval']

        msecs = secs * 1000 #convert to miliseconds
        self._timer = timer2.apply_interval(msecs, self.queue_run)

    def deactivate(self):
        super(collect_plugin, self).deactivate()
        if self._timer:
            self._timer.cancel()

    def queue_run(self):
        pub.sendMessage("run_queue", item=self.run)

    def run(self):
        raise NotImplemented


