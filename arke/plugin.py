
import json

from bson import json_util
from yapsy.IPlugin import IPlugin
import timer2

import config

class collect_plugin(IPlugin):
    name = None
    serialize = None
    custom_schema = False
    timestamp_as_id = False

    default_config = {'interval': 30}

    @property
    def config(self):
        try:
            return config.get_config()
        except config.NoSettings:
            return None

    def get_setting(self, setting, fallback=None):
        if self.config and self.config.has_option('plugin:%s' % self.name, setting):
            return self.config.get('plugin:%s' % self.name, setting)
        else:
            return self.default_config.get(setting, fallback)

    def __init__(self):
        IPlugin.__init__(self)
        self._timer = None
        assert 'interval' in self.default_config, (
            "Missing default interval value for %s plugin" % self.name)

    def activate(self):
        super(collect_plugin, self).activate()

        secs = self.get_setting('interval')
        msecs = secs * 1000 #convert to miliseconds
        self._timer = timer2.apply_interval(msecs, self.queue_run)

        self.queue_run()

    def deactivate(self):
        super(collect_plugin, self).deactivate()
        if self._timer:
            self._timer.cancel()

    def queue_run(self):
        config.queue_run(item=('gather_data', self))

    def __call__(self):
        if self.serialize and self.serialize.lower() == "json":
            return json.dumps(self.run())
        elif self.serialize and self.serialize.lower() == "extjson":
            return json.dumps(self.run(), default=json_util.default)
        else:
            return self.run()

    def run(self):
        raise NotImplemented


