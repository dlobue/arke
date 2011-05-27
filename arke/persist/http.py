
import httplib

from .base import ipersist

class http_backend(ipersist):
    def __init__(self, *args, **kwargs):
        super(http_backend, self).__init__(*args, **kwargs)
        self.host = self.config.get(self.section, 'host')
        self.port = None
        if self.config.has_option(self.section, 'port'):
            self.port = self.config.get(self.section, 'port')

    def get_connection(self):
        return httplib.HTTPConnection(self.host, self.port)

    def write(self, sourcetype, timestamp, data, hsh, hostname):
        conn = self.get_connection()
        uri = '/store/%s/%s/%s' % (hostname, sourcetype, timestamp)
        #headers = {"Content-type": "", "Accept": "text/plain"}
        conn.request('PUT', uri, body=data)
        resp = conn.getresponse()

        assert resp.status == 200, "Didn't get 200 from remote server"
        assert resp.read() == hsh, "hash of data received by remote server differs from our hash"

        return True

