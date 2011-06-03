
import httplib
import logging

from .base import ipersist

class http_backend(ipersist):
    content_type = {'json': 'application/json'}
    def __init__(self, *args, **kwargs):
        super(http_backend, self).__init__(*args, **kwargs)
        self.host = self.config.get(self.section, 'host')
        self.port = None
        if self.config.has_option(self.section, 'port'):
            self.port = self.config.get(self.section, 'port')

    def get_connection(self):
        return httplib.HTTPConnection(self.host, self.port)

    def write(self, sourcetype, timestamp, data, hostname, extra):
        conn = self.get_connection()
        uri = '/store/%s/%s/%s' % (hostname, sourcetype, timestamp)

        assert type(extra) is dict
        headers = extra
        if headers['ctype'] and "Content-type" not in headers:
            headers['Content-type'] = self.ctype_map[headers['ctype']]

        conn.request('PUT', uri, body=data, headers=headers)
        resp = conn.getresponse()

        if resp.status == 200:
            return True
        else:
            logging.warning("Didn't get 200 from remote server!")
            return False

