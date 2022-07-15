
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

class NoPlugins(Exception): pass

class PluginManager(object):

    def __init__(self,
                 base_class,
                 config,
                 entry_points=None,
                 init_args=None,
                 init_kwargs=None):

        self.config = config
        self._init_args = init_args or tuple()
        self._init_kwargs = init_kwargs or dict()
        self._base_class = base_class
        self._entry_points = entry_points
        self._modules = set()
        self._plugins = set()

    def load_entry_points(self, entry_points=None):
        if not ENTRY_POINTS:
            logger.debug("entry points disabled - unable to import pkg_resources")
            return

        if entry_points is None:
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
                    logger.exception(
                        f"Error loading plugin {entry.name} from {entry.dist.location}"
                    )

    def load_plugin_dirs(self, plugin_dirs=None):
        if plugin_dirs is None:
            plugin_dirs = self.config.get('core', 'plugin_dirs', None)
            if plugin_dirs is None:
                logger.warn('No plugin dirs given. skipping directory search.')
                return

            plugin_dirs = plugin_dirs.replace(',', ' ')
            plugin_dirs = plugin_dirs.split()

        if not hasattr(plugin_dirs, '__iter__'):
            plugin_dirs = [plugin_dirs]

        for path in plugin_dirs:
            logger.debug(f"searching for plugins in {path}")
            for py_file in glob(os.path.join(path, '*.py')):
                try:
                    module_name = os.path.basename(py_file[:-3])
                    if module_name.startswith('_'):
                        continue
                    if module_name in sys.modules:
                        self._modules.add(sys.modules[module_name])
                        continue

                    logger.debug(f"Loading module {py_file}")
                    module = imp.load_source(module_name, py_file)
                    self._modules.add(module)
                except Exception:
                    logger.exception(f"Error loading module {os.path.join(path, py_file)}")


    def load(self, base_class=None, **kwargs):
        if base_class is None:
            base_class = self._base_class

        def component_filter(member, module):
            return ( isclass(member) \
                    and issubclass(member, base_class) \
                    and getmodule(member) is module \
                    and not member.__name__.startswith('_')
                   )

        self.load_entry_points()
        self.load_plugin_dirs()

        logger.debug("in plugin_manager load. modules found: %r" % self._modules)

        plugins = self._plugins

        plugins.update((x[1](*self._init_args, **self._init_kwargs)
                        for module in self._modules
                        for x in getmembers(module,
                                          partial(component_filter, module=module))))

        logger.debug("in plugin_manager load. plugins found: %r" % plugins)



class CollectPlugins(PluginManager):

    def load(self, **kwargs):
        super(CollectPlugins, self).load(**kwargs)

        no_plugins_activated = True
        for plugin in self._plugins.copy():
            if not plugin.enabled:
                if plugin.is_activated:
                    logger.info(("active plugin %s is no longer enabled "
                                  "- deactivating.") % plugin.name)
                    plugin.deactivate()
                else:
                    logger.debug(("discovered plugin %s is not enabled. "
                               "skipping activation") % plugin.name)
                continue

            if not plugin.is_activated:
                logger.info(f"activating plugin {plugin.name}")
                if 'pool' in kwargs:
                    logger.debug(f"running activate method of plugin {plugin.name} in greenlet")
                    kwargs['pool'].spawn(plugin.activate)
                else:
                    plugin.activate()
            no_plugins_activated = False

        if no_plugins_activated:
            raise NoPlugins("No plugins found or enabled.")


class PersistPlugins(PluginManager):

    def load(self, **kwargs):
        super(CollectPlugins, self).load(**kwargs)

        if 'backend' in kwargs:
            backend = kwargs['backend']
        else:
            backend = self.config.get('core', 'persist_backend', None)

        if backend is None:
            logger.error("no backend configured!")
            raise SystemExit

        if backend in self:
            logger.debug("tried loading an already loaded backend.")
            return

        if any(filter(lambda x: x in self, self._plugins)):
            logger.warn("already have a backend loaded.")
            return

        plugins = [ x for x in self._plugins if x.name == backend ]
        if not plugins:
            logger.error(f"Requested backend {backend} not found!")
            return #XXX do something better
        if len(plugins) > 1:
            logger.error("found multiple plugins with that name!")
            return #XXX do something better

        plugins[0].register(self)


