
from time import time
import logging
from ConfigParser import SafeConfigParser
from os.path import expanduser
from socket import error, timeout

import eventlet
import boto
from paramiko.transport import Transport, SSHException

from arke.plugin import collect_plugin, config
from arke.util import partial

def get_credentials():
    confp = SafeConfigParser()
    confp.read(expanduser('~/.s3cfg'))
    return (confp.get('default', 'access_key'),
                     confp.get('default', 'secret_key'))

class ssh_hello(collect_plugin):
    name = "ssh_hello"
    format = 'json'
    hostname = None
    custom_schema = True
    timestamp_as_id = True
    default_config = {'interval': 10,
                      'port': 22,
                     }

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
            sock = eventlet.connect((host, self.get_setting('port')))

            transport = Transport(sock)
            transport.logger.setLevel(logging.WARNING)
            transport.packetizer.write_all(transport.local_version + '\r\n')
            transport._check_banner()
            transport.packetizer.close()

            lag = time() - start
        except error, e:
            if type(e) is timeout:
                log = logging.warn
            else:
                log = logging.error
            log('socket %s: errno=%r, error msg=%s for server %s' % (e.__class__.__name__, e.errno, e.strerror, server['fqdn']))
            lag = -1
        except SSHException, e:
            logging.warn('ssh exception: %s for server %s' % (e, server['fqdn']))
            lag = -1

        d = {'$addToSet':
             {'data':
              {'from': self.hostname,
               'to': server['fqdn'],
               'lag': lag}
             }
            }
        return d


if __name__ == '__main__':
    from sys import argv
    port = 22
    try:
        host = argv[1]
    except IndexError:
        host = 'localhost'
    else:
        try:
            port = argv[2]
        except IndexError:
            pass

    ssh_hello.default_config['port'] = port

    from pprint import pprint
    p = ssh_hello()
    p.hostname = 'localhost'
    pprint(p.run({'fqdn': host}))

