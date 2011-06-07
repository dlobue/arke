
#from functools import partial
from time import time
import json
import logging
from ConfigParser import SafeConfigParser
from os.path import expanduser
from socket import error, timeout, gaierror

from bson import json_util
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
    serialize = 'extjson'
    hostname = None
    custom_schema = True
    timestamp_as_id = True
    default_config = {'interval': 10,
                      'port': 64007}

    def activate(self):
        super(latency, self).activate()
        self._start_server()

    def _start_server(self):
        def handler(sock, client_addr):
            logging.debug("Got connection from: %s" % ':'.join(map(str, client_addr)))
            sock.recv(5)
            sock.sendall('PONG\n')
        eventlet.spawn_n(eventlet.serve, eventlet.listen(('0.0.0.0', self.get_setting('port'))), handler)

    def queue_run(self):
        sdb = boto.connect_sdb(*get_credentials())
        domain = sdb.get_domain('chef')
        servers = domain.select('select fqdn,ec2_public_hostname from chef where fqdn is not null')
        for server in servers:
            config.queue_run(item=('gather_data', partial(self, server)))

    def __call__(self, server):
        if self.serialize and self.serialize.lower() == "json":
            return json.dumps(self.run(server))
        elif self.serialize and self.serialize.lower() == "extjson":
            return json.dumps(self.run(server), default=json_util.default)
        else:
            return self.run(server)

    def run(self, server):
        if 'ec2_public_hostname' in server:
            host = server['ec2_public_hostname']
        else:
            host = server['fqdn']

        start = time()
        try:
            sock = eventlet.connect((host, self.get_setting('port')))
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

