
import json

from yapsy.IPlugin import IPlugin
import timer2

import config

class collect_plugin(IPlugin):
    name = None
    default_config = {'interval': 30}
    serialize = None

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

        self.queue_run()

    def deactivate(self):
        super(collect_plugin, self).deactivate()
        if self._timer:
            self._timer.cancel()

    def queue_run(self):
        config.queue_run(item=('gather_data', (self.name, self._run)))

    def _run(self):
        if self.serialize and self.serialize.lower() == "json":
            return ('json', json.dumps(self.run()))
        else:
            return ('raw', self.run())

    def run(self):
        raise NotImplemented


