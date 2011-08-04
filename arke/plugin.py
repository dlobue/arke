
import json
from time import time
import logging
from glob import glob
import imp
import os
import sys
from functools import partial
from inspect import getmembers, getmodule, isclass

from pprint import pformat

try:
    from pkg_resources import working_set
    ENTRY_POINTS = True
except ImportError:
    working_set = None
    ENTRY_POINTS = False

from bson import json_util, BSON
from circuits import Component, Event, Timer



class collect_plugin(Component):
    default_config = {'interval': 30}
    config = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(collect_plugin, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance

    def get_setting(self, setting, fallback=None, opt_type=None):
        val = None
        logging.debug("getting settings")
        if self.manager is not self or 'config' in self.components:
            try:
                #getter = {int: self.config.getint,
                          #float: self.config.getfloat,
                          #bool: self.config.getboolean,
                         #}[opt_type]
                getter = {int: 'getint',
                          float: 'getfloat',
                          bool: 'getboolean',
                         }[opt_type]
            except KeyError:
                getter = 'get'
            v = self.root.call(Event(self.section, setting, default=fallback), getter, target='config')
            val = v.value
            #val = v
            logging.debug("setting value from config component is %r" % val)

        if val is None:
            logging.debug("setting value is None. using defaults.")
            val = self.default_config.get(setting, fallback)
            if not (opt_type in (None, bool) or isinstance(val, opt_type)):
                try:
                    val = opt_type(val)
                except ValueError:
                    pass

        return val

    def __init__(self):
        super(collect_plugin, self).__init__()
        self.channel = self.name
        self.section = 'plugin:%s' % self.name
        self._timer = None
        assert 'interval' in self.default_config, (
            "Missing default interval value for %s plugin" % self.name)

    @property
    def enabled(self):
        return self.get_setting('enabled', False, opt_type=bool)

    @property
    def is_registered(self):
        if self.manager is not self and self in self.manager:
            return True
        return False

    def registered(self, component, manager):
        if manager is not self and manager is self.manager and component is self:
            secs = self.get_setting('interval', opt_type=int)
            self._timer = Timer(secs, Event(), 'gather_data', t=self.name, persist=True).register(self)

    def unregistered(self, component, manager):
        if manager is not self and manager is self.manager and component is self:
            if self._timer:
                self._timer.unregister()
                self._timer = None


    def serialize(self, data):
        if not self.format:
            return data

        if self.format.lower() == "json":
            return json.dumps(data)
        elif self.format.lower() == "bson":
            return BSON.encode(data)
        elif self.format.lower() == "extjson":
            return json.dumps(data, default=json_util.default)

    def gather_data(self):
        timestamp = time()
        sourcetype = self.name
        key = '%f%s' % (timestamp, sourcetype)
        extra = {}

        try:
            data = self.collect()
        except Exception, e:
            logging.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
            raise e


        #TODO: put stuff in spool
        #XXX: use spool as queue

        #logging.debug("sourcetype: %r, timestamp: %r, extra: %r, data:\n%s" % (sourcetype, timestamp, pformat(extra), pformat(data)))
        logging.debug("sourcetype: %r, timestamp: %r, extra: %r" % (sourcetype, timestamp, pformat(extra)))
        #spool[key] = (sourcetype, timestamp, data, extra)
        
        #self.persist_queue.put(key)

    #def gather_data(self):
        #timestamp = time()
        #sourcetype = self.name
        ##key needs to be generated before we normalize
        #key = '%f%s' % (timestamp, sourcetype)
        #extra = {}

        #if self.timestamp_as_id:
            ##if the timestamp is going to be used as the id, then
            ##that means we're going to group multiple results into
            ##the same document. round the timestamp in order to ensure
            ##we catch everything.
            #offset = timestamp % self.get_setting('interval')
            #timestamp = timestamp - offset
            #extra['timestamp_as_id'] = True

        #if self.custom_schema:
            #extra['custom_schema'] = True

        #if self.format:
            #extra['ctype'] = self.format.lower()

        #logging.debug("gathering data for %s sourcetype" % sourcetype)
        #try:
            #data = self.serialize(self.collect())
            ##data = plugin()
        #except Exception, e:
            #logging.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
            #raise e


        ##TODO: put stuff in spool
        ##XXX: use spool as queue

        #spool[key] = (sourcetype, timestamp, data, extra)
        
        #self.persist_queue.put(key)





class plugin_manager(Component):
    channel = "plugin_manager"

    def __init__(self,
                 init_args=None,
                 init_kwargs=None,
                 paths=None,
                 entry_points=None,
                 channel=channel):

        super(plugin_manager, self).__init__(channel=channel)

        self._init_args = init_args or tuple()
        self._init_kwargs = init_kwargs or dict()
        self._paths = paths
        self._entry_points = entry_points
        self._modules = set()
        self._plugins = set()

    def load_entry_points(self):
        if not ENTRY_POINTS:
            logging.debug("entry points disabled - unable to import pkg_resources")
            return

        entry_points = self._entry_points
        if entry_points is None:
            return
        if not hasattr(entry_points, '__iter__'):
            entry_points = [entry_points]
            
        for entry_point_id in entry_points:
            for entry in working_set.iter_entry_points(entry_point_id):
                logging.debug('Loading plugin %s from %s', entry.name, entry.dist.location)

                try:
                    module = entry.load(require=True)
                    self._modules.add(module)
                except:
                    logging.exception("Error loading plugin %s from %s" % (entry.name, entry.dist.location))

    def load_paths(self):
        paths = self._paths
        if not hasattr(paths, '__iter__'):
            paths = [paths]
            
        for path in paths:
            logging.debug("searching for plugins in %s" % path)
            for py_file in glob(os.path.join(path, '*.py')):
                try:
                    module_name = os.path.basename(py_file[:-3])
                    if module_name.startswith('_'):
                        continue
                    if module_name in sys.modules:
                        self._modules.add(sys.modules[module_name])
                        continue
                    
                    logging.debug("Loading module %s" % py_file)
                    module = imp.load_source(module_name, py_file)
                    self._modules.add(module)
                except Exception:
                    logging.exception("Error loading module %s" % os.path.join(path, py_file))


    def load(self, base_class):
        def component_filter(member, module):
            return ( isclass(member) \
                    and issubclass(member, base_class) \
                    and getmodule(member) is module \
                    and not member.__name__.startswith('_')
                   )

        self.load_entry_points()
        self.load_paths()

        plugins = self._plugins

        plugins.update((x[1](*self._init_args, **self._init_kwargs)
                        for module in self._modules
                        for x in getmembers(module,
                                          partial(component_filter, module=module))))

        logging.info("in plugin_manager load. modules contains: %r" % self._modules)
        logging.info("in plugin_manager load. plugins contains: %r" % plugins)

        for plugin in plugins:
            #logging.debug("number of events in self: %r, in manager: %r, in root: %r" % (len(self), len(self.manager), len(self.root)))
            #e = Event(plugin.section, 'enabled')
            #val = self.fire(e, 'getboolean', target='config')
            #self.manager.wait(e)
            val = self.manager.call(Event(plugin.section, 'enabled'), 'getboolean', target='config')
            #logging.debug("number of events in self: %r, in manager: %r, in root: %r" % (len(self), len(self.manager), len(self.root)))
            #while self: self.flush()
            #while self.manager: self.manager.flush()
            #logging.debug("number of events in self: %r, in manager: %r, in root: %r" % (len(self), len(self.manager), len(self.root)))
            #logging.info("iterating through plugins during load - val: %r, val.value: %r, e: %r" % (val, val.value, e))
            if val.value:
            #if self.config.getboolean(plugin.section, 'enabled'):
                if plugin not in self:
                    plugin.register(self)

            elif plugin in self:
                plugin.unregister()



