
from .base import ipersist
import pymongo

class mongo_backend(ipersist):
    def __init__(self):
        self._conn = pymongo.Connection

    def write(self, sourcetype, timestamp, data, hsh, hostname):
        pass

