
from os import path, makedirs
from cPickle import dumps, loads
import logging

logger = logging.getLogger(__name__)

import bsddb3 as bsddb
db = bsddb.db

class Spooler(object):
    def __init__(self, config):
        self.config = config
        self._txn_registry = {}
        self._dbenv = None
        self._db = None


    def open(self, spool_file=None):
        if self._db is not None or self._dbenv is not None:
            logger.warn("Told to open an already-open spool again!")
            return

        if spool_file is None:
            spool_file = self.config.get('core', 'spool_file', None)
            assert spool_file is not None, "no spool dir was given!"

        if '~' in spool_file:
            spool_file = path.expanduser(spool_file)
        spool_file = path.abspath(spool_file)
        spool_dir = path.dirname(spool_file)
        if not path.exists(spool_dir):
            makedirs(spool_dir)
        else:
            assert path.isdir(spool_dir), "%s exists and is not a dir! unable to initialize spool" % spool_dir

        e = bsddb.db.DBEnv()
        e.set_cachesize(0, 20480)
        e.set_lk_detect(db.DB_LOCK_DEFAULT)
        e.open(spool_dir, db.DB_PRIVATE | db.DB_CREATE | db.DB_THREAD | db.DB_INIT_LOCK | db.DB_INIT_MPOOL | db.DB_INIT_TXN)

        d = db.DB(e)
        d.set_q_extentsize(4096)
        d.set_pagesize(4096)

        d.open(spool_file, db.DB_RECNO, db.DB_CREATE, 0600)
        #d.open(spool_file, db.DB_RECNO, db.DB_CREATE | db.DB_AUTO_COMMIT, 0600)

        self._dbenv = e
        self._db = d
        return self

    def keys(self):
        return self._db.keys()

    def close(self):
        self._db.sync()
        self._db.close()
        self._dbenv.close()
        self._db = None
        self._dbenv = None

    def append(self, data):
        s = dumps(data)
        rid = self._db.append(s)
        return rid

    def delete(self, rid):
        return self._db.delete(rid)

    def get(self, rid):
        data = self._db.get(rid)
        return loads(data)

