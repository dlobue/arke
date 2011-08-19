
import logging
from datetime import datetime
from time import time, sleep
from socket import error, timeout

logger = logging.getLogger(__name__)

import boto

from arke.collect import Collect

BATCH_MAX_WAIT = 2
BATCH_INTERVAL = .1

class MultiCollect(Collect):
    normalize = True
    format = 'extjson'
    default_config = {'interval': 10,
                      'region': None,
                     }


    def collect(self, server, hostname):
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

        return {'from': hostname,
                'to': server['fqdn'],
                'lag': lag
               }

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
        logger.debug('looking for peers with the query: %s' % query)
        servers = self.sdb_domain.select(query)
        for server in servers:
            yield server

    def gather_data(self):
        timestamp = datetime.utcnow()
        sourcetype = self.name
        extra = {'timestamp_as_id': True,
                }

        #normalize timestamp so we can sync up with other servers
        second = timestamp.second - (timestamp.second % self.get_setting('interval', opt_type=int))
        timestamp = timestamp.replace(second=second, microsecond=0)

        hostname = self.config.get('core', 'hostname')
        logger.debug("sourcetype: %r, timestamp: %s, extra: %r" % (sourcetype, timestamp, extra))

        data_batch = []

        def _persist(data):
            self.spool.append(sourcetype, timestamp, data, extra)

        persist_handler = data_batch.append

        def _gather(server):
            if server['fqdn'] == hostname:
                return
            try:
                data = self.collect(server, hostname)
            except Exception:
                logger.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
                return
            persist_handler(data)

        total_servers = 0
        pool = self._pool
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
        logger.debug("Batched %i replies out of a total of %i" % (len(data_batch), total_servers))
        _persist(data_batch)




    def _format(self, data):
        if hasattr(data, '__iter__') and not hasattr(data, 'keys'):
            data = {'$each': data }
        d = {'$addToSet':
             {'data':
              data
             }
            }
        return d
