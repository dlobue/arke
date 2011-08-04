
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

from circuits import Component, Event




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


