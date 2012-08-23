
import logging
from datetime import datetime
from time import time, sleep
from socket import error, timeout

logger = logging.getLogger(__name__)

import boto

from arke.collect import Collect
from arke.childpool import KiddiePool

BATCH_MAX_WAIT = 2
BATCH_INTERVAL = .1

class MultiCollect(Collect):
    normalize = True
    format = 'extjson'
    default_config = {'interval': 10,
                      'region': None,
                      'parallelism': 10,
                      'datapoints': 20,
                     }


    def collect(self, server):
        if 'ec2_public_hostname' in server:
            host = server['ec2_public_hostname']
        else:
            host = server['fqdn']

        start = time()
        try:
            lag = self._collect(server, start, host)
        except error, e:
            if type(e) is timeout:
                log = logger.warn
            else:
                log = logger.error
            log('socket %s: errno=%r, error msg=%s for server %s' % (e.__class__.__name__, e.errno, e.strerror, server['fqdn']))
            lag = -1

        return dict(to=server['fqdn'],
                    lag=lag
                   )

    def iter_servers(self):
        if not hasattr(self, 'sdb_domain'):
            sdb = boto.connect_sdb()
            log = logging.getLogger('boto')
            log.setLevel(logging.INFO)
            self.sdb_domain = sdb.get_domain('chef')

        query = 'select fqdn,ec2_public_hostname from chef where fqdn is not null'
        region = self.get_setting('region')
        if region:
            query += " and ec2_region = '%s'" % region
        query += " order by fqdn"
        logger.debug('looking for peers with the query: %s' % query)

        hostname = self.config.get('core', 'hostname')
        collection = remainder = []
        top = []

        for server in self.sdb_domain.select(query):
            if server['fqdn'] == hostname:
                collection = top
                continue
            collection.append(server)
        collection.extend(remainder)
        del remainder


        datapoints = self.get_setting('datapoints', opt_type=int)
        if datapoints >= len(collection):
            interval = 1
            begin = 0
        else:
            interval = len(collection)/datapoints
            if not hasattr(self, '_interval_set'):
                self._interval_set = 0
            elif self._interval_set >= interval:
                self._interval_set = 0
            else:
                self._interval_set += 1
            begin = self._interval_set

        for i in xrange(begin, datapoints*interval, interval):
            yield collection[i]

    def gather_data(self):
        timestamp = datetime.utcnow()
        sourcetype = self.name
        extra = dict(multi='to') # multi is a key in the resulting data that can
                                 # be used to make the record id unique

        #normalize timestamp so we can sync up with other servers
        second = timestamp.second - (timestamp.second % self.get_setting('interval', opt_type=int))
        timestamp = timestamp.replace(second=second, microsecond=0)

        logger.debug("sourcetype: %r, timestamp: %s, extra: %r" % (sourcetype, timestamp, extra))

        data_batch = []

        def _persist(data):
            self.spool.append(sourcetype, timestamp, extra, data)

        persist_handler = data_batch.append

        def _gather(server):
            try:
                data = self.collect(server)
            except Exception:
                logger.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
                return
            persist_handler(data)

        total_servers = 0
        pool = KiddiePool(self._pool, self.get_setting('parallelism', opt_type=int))
        for server in self.iter_servers():
            total_servers += 1
            pool.spawn(_gather, server)

        c = 0
        while c < BATCH_MAX_WAIT:
            if len(data_batch) == total_servers:
                break
            sleep(BATCH_INTERVAL)
            c += BATCH_INTERVAL

        persist_handler = _persist
        if data_batch:
            logger.debug("Batched %i replies out of a total of %i" % (len(data_batch), total_servers))
            self.spool.extend(sourcetype, timestamp, extra, data_batch)

