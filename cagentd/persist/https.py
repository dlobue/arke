
import httplib

from .base import ipersist

class https_backend(ipersist):
    def __init__(self, *args, **kwargs):
        super(https_backend, self).__init__()
        self.host = self.config_parser.get(self.section, 'host')
        self.port = self.config_parser.get(self.section, 'port')

    def write(self, sourcetype, timestamp, data, hsh, hostname):
        conn = httplib.HTTPSConnection(self.host)
        uri = '/store/%s/%s/%s' % (hostname, sourcetype, timestamp)
        conn.request('PUT', uri, body=data)
        resp = conn.getresponse()

        assert resp.status == 200, "Didn't get 200 from remote server"
        assert resp.read() == hsh, "hash of data received by remote server differs from our hash"

        return True

