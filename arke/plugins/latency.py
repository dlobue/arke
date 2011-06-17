
from time import time
import logging
from ConfigParser import SafeConfigParser
from os.path import expanduser
from socket import error, timeout, gaierror

import eventlet
import boto

from arke.plugin import collect_plugin, config
from arke.util import partial

def get_credentials():
    confp = SafeConfigParser()
    confp.read(expanduser('~/.s3cfg'))
    return (confp.get('default', 'access_key'),
                     confp.get('default', 'secret_key'))

class latency(collect_plugin):
    name = "latency"
    format = 'extjson'
    hostname = None
    custom_schema = True
    timestamp_as_id = True
    default_config = {'interval': 10,
                      'port': 64007,
                      'server_concurrency': 1000,
                      'server_backlog': 2048,
                     }

    def activate(self):
        super(latency, self).activate()
        self._start_server()

    def _start_server(self):
        def handler(sock, client_addr):
            logging.debug("Got connection from: %s" % ':'.join(map(str, client_addr)))
            sock.recv(5)
            sock.sendall('PONG\n')
        eventlet.spawn_n(eventlet.serve,
                         eventlet.listen(('0.0.0.0', int(self.get_setting('port'))),
                                         backlog=int(self.get_setting('server_backlog'))),
                         handler,
                        concurrency=int(self.get_setting('server_concurrency')))

    def queue_run(self):
        if not self.hostname:
            self.hostname = self.config.get('core', 'hostname')

        if not hasattr(self, 'sdb_domain'):
            sdb = boto.connect_sdb(*get_credentials())
            log = logging.getLogger('boto')
            log.setLevel(logging.INFO)
            self.sdb_domain = sdb.get_domain('chef')

        domain = self.sdb_domain.get_domain('chef')
        servers = domain.select('select fqdn,ec2_public_hostname from chef where fqdn is not null')
        for server in servers:
            if server['fqdn'] == self.hostname:
                continue
            config.queue_run(item=('gather_data', partial(self, server)))

    def __call__(self, server):
        return self.serialize(self.run(server))

    def run(self, server):
        if 'ec2_public_hostname' in server:
            host = server['ec2_public_hostname']
        else:
            host = server['fqdn']

        start = time()
        try:
            sock = eventlet.connect((host, int(self.get_setting('port'))))
            sock.sendall('PING\n')
            sock.recv(5)
            lag = time() - start
        except error, e:
            logging.error('socket error: %s for server %s' % (e, server['fqdn']))
            lag = -1
        except gaierror, e:
            logging.error('socket gaierror: %s for server %s' % (e, server['fqdn']))
            lag = -1
        except timeout, e:
            logging.warn('socket timeout: %s for server %s' % (e, server['fqdn']))
            lag = -1

        if not self.hostname:
            self.hostname = self.config.get('core', 'hostname')

        d = {'$addToSet':
             {'data':
              {'from': self.hostname,
               'to': server['fqdn'],
               'lag': lag}
             }
            }
        return d


if __name__ == '__main__':
    from pprint import pprint
    p = latency()
    p.hostname = 'localhost'
    p._start_server()
    pprint(p.run({'fqdn': 'localhost'}))

