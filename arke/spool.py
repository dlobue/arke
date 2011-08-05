
from os import path, makedirs
from cPickle import dumps, loads
import logging

logger = logging.getLogger(__name__)

import bsddb
db = bsddb.db
from circuits import Component, Event, handler

class Spooler(Component):
    def __init__(self):
        super(Spooler, self).__init__()

        self._txn_registry = {}
        self._dbenv = None
        self._db = None


    def serialize(self, data):
        return dumps(data)
    def deserialize(self, data):
        return loads(data)

    @handler('started')
    def open(self, spool_file=None):
        if self._db is not None or self._dbenv is not None:
            logger.warn("Told to open an already-open spool again!")
            return

        if spool_file is None:
            spool_file = self.root.call(Event('core', 'spool_file'), 'get', target='config').value
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
        d.open(file, db.DB_RECNO, db.DB_CREATE, 0600)

        self._dbenv = e
        self._db = d


    @handler('stopped')
    def close(self):
        if self._txn_registry:
            logger.error("Told to close when there are still open transactions!")
            #XXX now what?
            raise RuntimeError

        self._db.close()
        self._dbenv.close()
        self._db = None
        self._dbenv = None



    @handler('persist_data', target='persist', override=True)
    def append(self, sourcetype, timestamp, data, extra=None):
        txn = self._dbenv.txn_begin()
        s = self.manager.serialize((sourcetype, timestamp, data, extra))
        rid = self._db.append(s, txn=txn)
        txn.commit()

        self.fire(Event(), 'persist', target='persist')
        return rid

    
    def get(self, record_id):
        self._db.get(record_id)

    def consume(self):
        txn = self._dbenv.txn_begin()
        rid, data = self._db.consume(txn=txn)
        self._txn_registry[rid] = txn


    def commit(self, rid):
        if rid not in self._txn_registry:
            logger.error("Was asked to commit a transaction for record id I have no record of: %r" % rid)
            return None
        txn = self._txn_registry.pop(rid)
        txn.commit()




