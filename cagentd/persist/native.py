
import pymongo

class native(object):
    def __init__(self):
        self._conn = pymongo.Connection

    def write(self, data):
        pass

