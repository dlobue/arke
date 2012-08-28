
from gevent import core
from gevent.httplib import HTTPConnection as _HTTPConnection
import gevent.httplib

class HTTPConnection(_HTTPConnection):
    def connect(self):
        if self.conn: return

        if self.debuglevel > 0:
            print 'connect: (%s, %u)' % (self.host, self.port)

        self.conn = core.http_connection.new(self.host, self.port)

        if self.timeout is not None:
            self.conn.set_timeout(int(max(1, self.timeout)))

gevent.httplib.HTTPConnection = HTTPConnection

