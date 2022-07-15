
import json
from datetime import datetime
from time import time
import logging
logger = logging.getLogger(__name__)

from bson import json_util, BSON

from gevent.core import timer

class Collect(object):
    default_config = {'interval': 30}
    normalize = False
    format = 'extjson'

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Collect, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance

    def get_setting(self, setting, fallback=None, opt_type=None):
        val = None
        if self.config is not None and self.config.has_option(self.section, setting):
            try:
                getter = {int: 'getint',
                          float: 'getfloat',
                          bool: 'getboolean',
                         }[opt_type]
            except KeyError:
                getter = 'get'
            val = getattr(self.config, getter)(self.section, setting)

        if val is None:
            #logger.debug("setting value is None. using defaults.")
            val = self.default_config.get(setting, fallback)
            if not (opt_type in (None, bool) or isinstance(val, opt_type)):
                try:
                    val = opt_type(val)
                except ValueError:
                    pass

        return val

    def __init__(self, config, persist_queue, spool, pool):
        self._pool = pool
        self.spool = spool
        self.config = config
        self.persist_queue = persist_queue
        self.name = self.__class__.__name__
        self.section = f'plugin:{self.name}'
        if not hasattr(self, '_timer'):
            self._timer = None
        if not hasattr(self, 'is_activated'):
            self.is_activated = False
        assert (
            'interval' in self.default_config
        ), f"Missing default interval value for {self.name} plugin"

    @property
    def enabled(self):
        return self.get_setting('enabled', False, opt_type=bool)

    def activate(self):
        self.is_activated = True
        self.run()

    def deactivate(self):
        self.is_activated = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _reset_timer(self):
        assert self._timer is None, "Trying to reset active timer!"

        t = time()
        s = self.get_setting('interval', opt_type=int)
        if self.normalize:
            s = (s - (t % s))

        self._timer = timer(s, self._pool.spawn, self.run)

    def run(self):
        self._timer = None
        logger.debug(f"Doing run for {self.name} plugin.")
        try:
            self.gather_data()
        finally:
            self._reset_timer()


    def gather_data(self):
        timestamp = datetime.utcnow()
        sourcetype = self.name
        extra = {
        }

        try:
            data = self.collect()
            logger.debug(f"Data collection for {self.name} plugin completed")
        except Exception:
            logger.exception(
                f"error occurred while gathering data for sourcetype {sourcetype}"
            )

            return

        logger.debug("sourcetype: %r, timestamp: %s, extra: %r" % (sourcetype, timestamp, extra))
        self.spool.append(sourcetype, timestamp, extra, data)

    def serialize(self, data):
        if not self.format:
            return data

        if self.format.lower() == "json":
            return json.dumps(data)
        elif self.format.lower() == "bson":
            return BSON.encode(data)
        elif self.format.lower() == "extjson":
            return json.dumps(data, default=json_util.default)

