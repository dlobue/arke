
import json

from bson import json_util, BSON
from circuits import Component, Event, Timer

class ipersist(object):
    def __init__(self, config_parser):
        self.config = config_parser
        self.section = 'backend:%s' % self.__class__.__name__.replace('_backend', '')

    def serialize(self, data):
        if not self.format:
            return data

        if self.format.lower() == "json":
            return json.dumps(data)
        elif self.format.lower() == "bson":
            return BSON.encode(data)
        elif self.format.lower() == "extjson":
            return json.dumps(data, default=json_util.default)

    def write(self, sourcetype, timestamp, data, hostname, extra):
        """
        If the write fails (tbd by implementation), return False so the
        runner knows to reschedule.
        Otherwise return True
        """
        raise NotImplemented

