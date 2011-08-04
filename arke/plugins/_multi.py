
import logging
from time import time
from socket import error, timeout

import boto

from arke.plugin import collect_plugin

class _multi_collect_plugin(collect_plugin):
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
                log = logging.warn
            else:
                log = logging.error
            log('socket %s: errno=%r, error msg=%s for server %s' % (e.__class__.__name__, e.errno, e.strerror, server['fqdn']))
            lag = -1

        return {'from': hostname,
                'to': server['fqdn'],
                'lag': lag
               }

    def format_data(self, data):
        if hasattr(data, '__iter__') and not hasattr(data, 'setdefault'):
            data = { '$each': data }
        d = {'$addToSet':
             {'data': data }
            }
        return d

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
        logging.debug('looking for peers with the query: %s' % query)
        servers = self.sdb_domain.select(query)
        for server in servers:
            yield server

    def gather_data(self):
        timestamp = time()
        sourcetype = self.name
        #key needs to be generated before we normalize
        key = '%f%s' % (timestamp, sourcetype)
        extra = {}

        offset = timestamp % self.get_setting('interval')
        timestamp = timestamp - offset
        extra['timestamp_as_id'] = True

        extra['custom_schema'] = True

        hostname = self.config.get('core', 'hostname')

        for server in self.iter_servers():
            if server['fqdn'] == hostname:
                continue
            try:
                data = self.collect(server, hostname)
                data = self.serialize(self.collect())
            except Exception:
                logging.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
                continue

            #TODO: put data in spool
            #TODO: serialize data
            #TODO: 'format' data
            #TODO: batch data together


        #TODO: put stuff in spool
        #XXX: use spool as queue

        spool[key] = (sourcetype, timestamp, data, extra)
        
        self.persist_queue.put(key)


