
from os import path, makedirs
from cPickle import dumps, loads
import logging

logger = logging.getLogger(__name__)

import bsddb3 as bsddb
db = bsddb.db
from circuits import Component, Event, handler

class Spooler(Component):
    channel = 'spooler'
    def __init__(self, spool_file=None):
        super(Spooler, self).__init__()

        self._txn_registry = {}
        self._dbenv = None
        self._db = None
        self._spool_file = spool_file


    @handler('started')
    def open(self, *args, **kwargs):
        if self._db is not None or self._dbenv is not None:
            logger.warn("Told to open an already-open spool again!")
            return

        spool_file = self._spool_file
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
        d.set_q_extentsize(4096)
        d.set_pagesize(4096)
        #d.set_re_len(4064)

        d.open(spool_file, db.DB_RECNO, db.DB_CREATE | db.DB_AUTO_COMMIT, 0600)

        self._dbenv = e
        self._db = d


    @handler('stopped')
    def close(self, *args, **kwargs):
        if self._txn_registry:
            logger.error("Told to close when there are still open transactions!")
            #XXX now what?
            raise SystemExit

        self._db.close()
        self._dbenv.close()
        self._db = None
        self._dbenv = None



    @handler('persist_data', target='persist', override=True)
    def append(self, sourcetype, timestamp, data, extra=None):
        s = dumps((sourcetype, timestamp, data, extra))
        rid = self._db.append(s)

        self.fire(Event(rid=(rid, (sourcetype, timestamp, data, extra))), 'persist', target='persist')
        return rid

    @handler('remove')
    def delete(self, rid):
        return self._db.delete(rid)

    def get(self, rid):
        data = self._db.get(rid)
        return loads(data)



    #@handler('persist_data', target='persist', override=True)
    #def append(self, sourcetype, timestamp, data, extra=None):
        #txn = self._dbenv.txn_begin()
        #s = self.serialize((sourcetype, timestamp, data, extra))
        #rid = self._db.append(s, txn=txn)
        #txn.commit()

        #self.fire(Event(rid=(rid, (sourcetype, timestamp, data, extra))), 'persist', target='persist')
        #return rid

    def pop(self, rid):
        txn = self._dbenv.txn_begin()
        data = self._db.get(rid, txn=txn)
        self._db.delete(rid, txn=txn)
        self._txn_registry[rid] = txn
        return loads(data)

    def commit(self, rid):
        if rid not in self._txn_registry:
            logger.error("Was asked to commit a transaction for record id I have no record of: %r" % rid)
            return None
        txn = self._txn_registry.pop(rid)
        txn.commit()




