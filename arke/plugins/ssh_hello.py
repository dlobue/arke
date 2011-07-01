
from time import time
import logging
from socket import error, timeout

import eventlet
from paramiko.transport import Transport, SSHException

from arke.plugin import multi_collect_plugin, config

class ssh_hello(multi_collect_plugin):
    name = "ssh_hello"
    format = 'json'
    hostname = None
    custom_schema = True
    timestamp_as_id = True
    default_config = {'interval': 10,
                      'port': 22,
                      'region': None,
                     }


    def run(self, server):
        if 'ec2_public_hostname' in server:
            host = server['ec2_public_hostname']
        else:
            host = server['fqdn']

        start = time()
        try:
            lag = self._run(server, start, host)
        except error, e:
            if type(e) is timeout:
                log = logging.warn
            else:
                log = logging.error
            log('socket %s: errno=%r, error msg=%s for server %s' % (e.__class__.__name__, e.errno, e.strerror, server['fqdn']))
            lag = -1

        d = {'$addToSet':
             {'data':
              {'from': self.hostname,
               'to': server['fqdn'],
               'lag': lag}
             }
            }
        return d

    def _run(self, server, start, host):
        try:
            sock = eventlet.connect((host, int(self.get_setting('port'))))

            transport = Transport(sock)
            transport.logger.setLevel(logging.WARNING)
            transport.packetizer.write_all(transport.local_version + '\r\n')
            transport._check_banner()
            transport.packetizer.close()

            lag = time() - start
        except SSHException, e:
            logging.warn('ssh exception: %s for server %s' % (e, server['fqdn']))
            lag = -1

        return lag



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

