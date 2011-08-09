
from time import time
import logging
logger = logging.getLogger(__name__)

from circuits import Component, Event, Timer

class Collect(Component):
    default_config = {'interval': 30}

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Collect, cls).__new__(
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
            #v = self.root.call(Event(self.section, setting, default=fallback), getter, target='config')
            val = getattr(self.root.config, getter)(self.section, setting, default=fallback)
            #val = v.value
            #logger.debug("setting value from config component is %r" % val)

        if val is None:
            #logger.debug("setting value is None. using defaults.")
            val = self.default_config.get(setting, fallback)
            if not (opt_type in (None, bool) or isinstance(val, opt_type)):
                try:
                    val = opt_type(val)
                except ValueError:
                    pass

        return val

    def __init__(self):
        super(Collect, self).__init__()
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
        extra = None

        logger.debug("sourcetype: %r, timestamp: %r, extra: %r" % (sourcetype, timestamp, extra))
        try:
            data = self.collect()
        except Exception:
            logger.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
            return

        self.root.fire(Event(data=data,
                             extra=extra,
                             timestamp=timestamp,
                             sourcetype=sourcetype),
                       'persist_data', target='persist')


