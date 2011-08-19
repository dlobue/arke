
import json

from bson import json_util, BSON

class ipersist(object):
    def __init__(self, config_parser):
        self.config = config_parser
        self.section = 'backend:%s' % self.__class__.__name__.replace('_backend', '')

    def batch_write(self, sourcetype, spool_file, hostname):
        raise NotImplemented

    def write(self, sourcetype, timestamp, data, hostname, extra):
        """
        If the write fails (tbd by implementation), return False so the
        runner knows to reschedule.
        Otherwise return True
        """
        raise NotImplemented

