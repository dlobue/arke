
import json

from bson import json_util, BSON
from giblets import Component, ComponentManager, ExtensionPoint, implements
from giblets.policy import Blacklist
from giblets.search import find_plugins_by_entry_point, find_plugins_in_path
import timer2

import config
from arke.interfaces import icollecter

def get_plugin_manager(search_paths=None):
    #mgr = PluginManager(ComponentManager())
    mgr = PluginManager()
    blacklist = Blacklist()
    blacklist.disable_component(collect_plugin)
    mgr.restrict(blacklist)
    find_plugins_by_entry_point('arke_plugins')
    if search_paths:
        find_plugins_in_path(search_paths)
    return mgr

#class PluginManager(Component):
    #plugins = ExtensionPoint(icollecter)

class PluginManager(ComponentManager):
    collection_plugins = ExtensionPoint(icollecter)
    compmgr = property(lambda self: self)
    

class collect_plugin(Component):
    implements(icollecter)
    format = None
    custom_schema = False
    timestamp_as_id = False

    default_config = {'interval': 30}

    @property
    def config(self):
        try:
            return config.get_config()
        except config.NoSettings:
            return None

    def get_setting(self, setting, fallback=None, opt_type=None):
        if self.config and self.config.has_option(self.section, setting):
            try:
                getter = {int: self.config.getint,
                          float: self.config.getfloat,
                          bool: self.config.getboolean,
                         }[opt_type]
            except KeyError:
                getter = self.config.get
            return getter(self.section, setting)
        else:
            val = self.default_config.get(setting, fallback)
            if opt_type in (None, bool) or isinstance(val, opt_type):
                return val
            try:
                val = opt_type(val)
            except ValueError:
                return val

    def __init__(self):
        self.is_activated = False
        self.name = self.__class__.__name__
        self.section = 'plugin:%s' % self.name
        self._timer = None
        assert 'interval' in self.default_config, (
            "Missing default interval value for %s plugin" % self.name)


    @property
    def enabled(self):
        return self.get_setting('enabled', False, opt_type=bool)

    def activate(self):
        self.is_activated = True

        secs = self.get_setting('interval', opt_type=int)
        msecs = secs * 1000 #convert to miliseconds
        self._timer = timer2.apply_interval(msecs, self.queue_run)

        self.queue_run()

    def deactivate(self):
        self.is_activated = False
        if self._timer:
            self._timer.cancel()

    def queue_run(self):
        config.queue_run(item=('gather_data', self))

    def __call__(self):
        return self.serialize(self.run())

    def serialize(self, data):
        if not self.format:
            return data

        if self.format.lower() == "json":
            return json.dumps(data)
        elif self.format.lower() == "bson":
            return BSON.encode(data)
        elif self.format.lower() == "extjson":
            return json.dumps(data, default=json_util.default)
            

    def run(self):
        raise NotImplemented


