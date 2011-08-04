
import json
from time import time
import logging
from glob import glob
import imp
import os
import sys


try:
    from pkg_resources import working_set
    ENTRY_POINTS = True
except ImportError:
    working_set = None
    ENTRY_POINTS = False

from bson import json_util, BSON
#from giblets import Component, ComponentManager, ExtensionPoint, implements
#from giblets.policy import Blacklist
#from giblets.search import find_plugins_by_entry_point, find_plugins_in_path
from circuits import Component, Event, Timer


class GatherData(Event): pass


def get_plugin_manager(search_paths=None):
    mgr = PluginManager()
    blacklist = Blacklist()
    blacklist.disable_component(collect_plugin)
    blacklist.disable_component(multi_collect_plugin)
    mgr.restrict(blacklist)
    find_plugins_by_entry_point('arke_plugins')
    if search_paths:
        find_plugins_in_path(search_paths)
    return mgr

#class PluginManager(ComponentManager):
    #collection_plugins = ExtensionPoint(icollecter)
    #compmgr = property(lambda self: self)


class collect_plugin(Component):
    #implements(icollecter)
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

    def registered(self):
        secs = self.get_setting('interval', opt_type=int)
        self._timer = Timer(secs, GatherData, self.channel, persist=True).register(self)

    def unregistered(self):
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
            #data = self.serialize(self.collect())
            #data = plugin()
        except Exception, e:
            logging.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
            raise e


        #TODO: put stuff in spool
        #XXX: use spool as queue

        spool[key] = (sourcetype, timestamp, data, extra)
        
        self.persist_queue.put(key)

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
                    # if it's already loaded, move on 
                    if module_name in sys.modules:
                        self._modules.add(sys.modules[module_name])
                        continue
                    
                    logging.debug("Loading module %s" % py_file)
                    module = imp.load_source(module_name, py_file)
                    self._modules.add(module)
                except Exception:
                    logging.exception("Error loading module %s" % os.path.join(path, py_file))




    def load(self, base_class):
        modules = self._modules
        module = safeimport(name)
        if module is not None:

            test = lambda x: isclass(x) \
                    and issubclass(x, BaseComponent) \
                    and getmodule(x) is module
            components = [x[1] for x in getmembers(module, test)]

            if components:
                TheComponent = components[0]

                component = TheComponent(
                    *self._init_args,
                    **self._init_kwargs
                )

                if self._auto_register:
                    component.register(self)

                return component


def find_plugins_in_path(search_path):
    """
    Discover plugins in any .py files in the given on-disk locations eg:
    
    find_plugins_in_path("/path/to/mymodule/plugins")
    find_plugins_in_path(["/path/to/mymodule/plugins", "/some/more/plugins"])
    
    """
    if isinstance(search_path, basestring):
        search_path = [search_path]

    for path in search_path:
        log.debug("searching for plugins in %s" % search_path)
        for py_file in glob(os.path.join(path, '*.py')):
            try:
                module_name = os.path.basename(py_file[:-3])
                # if it's already loaded, move on 
                if module_name in sys.modules:
                    continue
                
                log.debug("Loading module %s" % py_file)
                module = imp.load_source(module_name, py_file)
            except:
                log.error("Error loading module %s: %s" % (os.path.join(path, py_file), traceback.format_exc()))



    def find_plugins_by_entry_point(entry_point_id, ws=master_working_set):
        for entry in ws.iter_entry_points(entry_point_id):
            log.debug('Loading plugin %s from %s', entry.name, entry.dist.location)

            try:
                entry.load(require=True)
            except:
                import traceback
                log.error("Error loading plugin %s from %s: %s" % (entry.name, entry.dist.location, traceback.format_exc()))
        


