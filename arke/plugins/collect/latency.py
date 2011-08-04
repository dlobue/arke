
from time import time
import logging

from circuits import handler
from circuits.net.sockets import TCPServer, Write, Close

from gevent.socket import create_connection
from gevent.server import StreamServer
from gevent import spawn

from arke.plugins.collect._multi import _multi_collect_plugin

class Ponger(TCPServer):
    def read(self, sock, data):
        self.fire(Write(sock, 'PONG'))
        self.fire(Close(sock))


class latency(_multi_collect_plugin):
    hostname = None
    default_config = {'interval': 10,
                      'port': 64007,
                      'server_concurrency': 1000,
                      'server_backlog': 2048,
                      'region': None,
                     }

    #def activate(self):
        #super(latency, self).activate()
        #self._start_server()

    @handler("registered")
    def _start_server(self):
        if not hasattr(self, '_server') or self._server is None:
            self._server = Ponger(
                self.get_setting('port', opt_type=int),
                backlog=self.get_setting('server_backlog', opt_type=int)
            ).register(self)

    @handler("unregister")
    def _stop_server(self):
        if hasattr(self, '_server') and self._server is not None:
            self._server.stop()

    #def _start_server(self):
        #def handler(sock, client_addr):
            #logging.debug("Got connection from: %s" % ':'.join(map(str, client_addr)))
            #sock.recv(5)
            #sock.sendall('PONG\n')
        #self._server = StreamServer(
                         #('0.0.0.0', self.get_setting('port', opt_type=int)),
                         #handle=handler,
                         #backlog=self.get_setting('server_backlog', opt_type=int))
        #spawn(self._server.serve_forever)


    def _collect(self, server, start, host):
        sock = create_connection((host, self.get_setting('port', opt_type=int)))
        sock.sendall('PING\n')
        sock.recv(5)
        return time() - start



if __name__ == '__main__':
    from pprint import pprint
    #from giblets import ComponentManager
    #cm = ComponentManager()
    p = latency()
    p.hostname = 'localhost'
    p._start_server()
    pprint(p.collect({'fqdn': 'localhost'}))

