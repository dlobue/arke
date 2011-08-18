
from os import path, makedirs, remove, listdir
from time import time as _time
from uuid import uuid4
import logging
from Queue import Empty
from threading import Lock, Condition

logger = logging.getLogger(__name__)

class Spooler(object):
    def __init__(self, config):
        self._lock = Lock()
        self._not_empty = Condition(self._lock)
        self.config = config
        self._spool_dir = config.get('core', 'spool_dir')
        makedirs(self._spool_dir)
        self._open()

    def _open(self):
        with self._lock:
            self._file = open(path.join(self._spool_dir, uuid4().hex), 'a')

    def keys(self):
        spool_dir = self._spool_dir
        return (path.join(spool_dir, f) for f in listdir(spool_dir))

    def items(self):
        return ((f, open(f, 'r')) for f in self.keys())

    def values(self):
        return (v for k,v in self.items())

    def close(self):
        self._file.flush()
        self._file.close()

    def append(self, data):
        with self._lock:
            self._file.write(data + '\n')
        self._not_empty.notify()

    def delete(self, file_handle):
        fn = file_handle.name
        file_handle.close()
        remove(fn)

    def get(self, timeout=None):
        self._not_empty.acquire()
        try:
            _f = self._file
            if not _f.tell() and timeout is None:
                raise Empty
            else:
                assert isinstance(timeout, int) and timeout > 0
                endtime = _time() + timeout
                while not _f.tell():
                    remaining = endtime - _time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)

            self._open()
            _f.flush()
            return _f
        finally:
            self._not_empty.release()

