
from time import time
import logging
from socket import error, timeout

import eventlet

from arke.plugin import multi_collect_plugin, config

class latency(multi_collect_plugin):
    name = "latency"
    format = 'extjson'
    hostname = None
    custom_schema = True
    timestamp_as_id = True
    default_config = {'interval': 10,
                      'port': 64007,
                      'server_concurrency': 1000,
                      'server_backlog': 2048,
                      'region': None,
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
                         eventlet.listen(('0.0.0.0', self.get_setting('port', opt_type=int)),
                                         backlog=self.get_setting('server_backlog', opt_type=int)),
                         handler,
                        concurrency=self.get_setting('server_concurrency', opt_type=int))


    def _run(self, server, start, host):
        sock = eventlet.connect((host, self.get_setting('port', opt_type=int)))
        sock.sendall('PING\n')
        sock.recv(5)
        return time() - start


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


if __name__ == '__main__':
    from pprint import pprint
    from giblets import ComponentManager
    cm = ComponentManager()
    p = latency(cm)
    p.hostname = 'localhost'
    p._start_server()
    pprint(p.run({'fqdn': 'localhost'}))

