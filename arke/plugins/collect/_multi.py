
import logging
from time import time
from socket import error, timeout

logger = logging.getLogger(__name__)

import boto

from arke.collect import Collect

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
        timestamp = time()
        sourcetype = self.name
        extra = {'timestamp_as_id': True,
                 'custom_schema': True,
                 'ctype': self.format,
                }

        #normalize timestamp so we can sync up with other servers
        timestamp = timestamp - (timestamp % self.get_setting('interval', opt_type=int))

        hostname = self.config.get('core', 'hostname')
        logger.debug("sourcetype: %r, timestamp: %r, extra: %r" % (sourcetype, timestamp, extra))

        def _gather(server):
            if server['fqdn'] == hostname:
                return
            try:
                data = self.serialize(self.collect(server, hostname))
            except Exception:
                logger.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
                return
            key = self.spool.append((sourcetype, timestamp, data, extra))
            self.persist_queue.put(key)

        pool = self._pool
        for server in self.iter_servers():
            pool.spawn(_gather, server)

