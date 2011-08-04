
from time import time
import logging
from glob import glob
import imp
import os
import sys
from functools import partial
from inspect import getmembers, getmodule, isclass

logger = logging.getLogger(__name__)

try:
    from pkg_resources import working_set
    ENTRY_POINTS = True
except ImportError:
    working_set = None
    ENTRY_POINTS = False

from circuits import Component, Event, Timer


class collect_plugin(Component):
    default_config = {'interval': 30}

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(collect_plugin, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance

    def get_setting(self, setting, fallback=None, opt_type=None):
        val = None
        if self.manager is not self or 'config' in self.components:
            try:
                getter = {int: 'getint',
                          float: 'getfloat',
                          bool: 'getboolean',
                         }[opt_type]
            except KeyError:
                getter = 'get'
            v = self.root.call(Event(self.section, setting, default=fallback), getter, target='config')
            val = v.value
            logger.debug("setting value from config component is %r" % val)

        if val is None:
            logger.debug("setting value is None. using defaults.")
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
        if manager is not self and manager is self.manager \
           and component is self and self._timer is None:
            secs = self.get_setting('interval', opt_type=int)
            self._timer = Timer(secs, Event(), 'gather_data', t=self.name, persist=True).register(self)

    def unregistered(self, component, manager):
        if manager is not self and manager is self.manager \
           and component is self and self._timer is not None:
                self._timer.unregister()
                self._timer = None


    def gather_data(self):
        timestamp = time()
        sourcetype = self.name
        extra = {}

        try:
            data = self.collect()
        except Exception, e:
            logger.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
            raise e


        #TODO: put stuff in spool
        #XXX: use spool as queue

        #logger.debug("sourcetype: %r, timestamp: %r, extra: %r, data:\n%s" % (sourcetype, timestamp, pformat(extra), pformat(data)))
        logger.debug("sourcetype: %r, timestamp: %r, extra: %r" % (sourcetype, timestamp, extra))
        #spool[key] = (sourcetype, timestamp, data, extra)
        



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
            logger.debug("entry points disabled - unable to import pkg_resources")
            return

        entry_points = self._entry_points
        if entry_points is None:
            return
        if not hasattr(entry_points, '__iter__'):
            entry_points = [entry_points]
            
        for entry_point_id in entry_points:
            for entry in working_set.iter_entry_points(entry_point_id):
                logger.debug('Loading plugin %s from %s', entry.name, entry.dist.location)

                try:
                    module = entry.load(require=True)
                    self._modules.add(module)
                except:
                    logger.exception("Error loading plugin %s from %s" % (entry.name, entry.dist.location))

    def load_paths(self):
        paths = self._paths
        if not hasattr(paths, '__iter__'):
            paths = [paths]
            
        for path in paths:
            logger.debug("searching for plugins in %s" % path)
            for py_file in glob(os.path.join(path, '*.py')):
                try:
                    module_name = os.path.basename(py_file[:-3])
                    if module_name.startswith('_'):
                        continue
                    if module_name in sys.modules:
                        self._modules.add(sys.modules[module_name])
                        continue
                    
                    logger.debug("Loading module %s" % py_file)
                    module = imp.load_source(module_name, py_file)
                    self._modules.add(module)
                except Exception:
                    logger.exception("Error loading module %s" % os.path.join(path, py_file))


    def load(self, base_class):
        def component_filter(member, module):
            return ( isclass(member) \
                    and issubclass(member, base_class) \
                    and getmodule(member) is module \
                    and not member.__name__.startswith('_')
                   )

        self.load_entry_points()
        self.load_paths()

        logger.debug("in plugin_manager load. modules contains: %r" % self._modules)

        plugins = self._plugins

        plugins.update((x[1](*self._init_args, **self._init_kwargs)
                        for module in self._modules
                        for x in getmembers(module,
                                          partial(component_filter, module=module))))

        logger.debug("in plugin_manager load. plugins contains: %r" % plugins)

        for plugin in plugins:
            val = self.manager.call(Event(plugin.section, 'enabled'), 'getboolean', target='config')
            if val.value:
                if plugin not in self:
                    plugin.register(self)

            elif plugin in self:
                plugin.unregister()


