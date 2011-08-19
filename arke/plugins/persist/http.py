
import httplib
import logging
import json

logger = logging.getLogger(__name__)

from bson import json_util

from .base import ipersist

class http_backend(ipersist):
    content_type_map = {'json': 'application/json',
                        'extjson': 'application/extjson',
                        'bson': 'application/bson',
                       }

    def __init__(self, *args, **kwargs):
        super(http_backend, self).__init__(*args, **kwargs)
        self.host = self.config.get(self.section, 'host')
        self.port = None
        if self.config.has_option(self.section, 'port'):
            self.port = self.config.getint(self.section, 'port')
        if self.config.has_option('core', 'debug'):
            self.debug = self.config.getboolean('core', 'debug')
        else:
            self.debug = False

    def get_connection(self):
        return httplib.HTTPConnection(self.host, self.port)

    def write(self, sourcetype, timestamp, data, hostname, extra):
        conn = self.get_connection()
        uri = '/store/%s/%s/%f' % (hostname, sourcetype, timestamp)

        headers = {}
        if extra and isinstance(extra, dict):
            if extra.get('ctype', None):
                headers['Content-type'] = self.content_type_map[extra['ctype']]
            headers['extra'] = json.dumps(extra, default=json_util.default)

        conn.request('POST', uri, body=data, headers=headers)
        resp = conn.getresponse()

        assert resp.status in (200,204), "Didn't get 200 from remote server!"


    def batch_write(self, sourcetype, spool_file, hostname):
        conn = self.get_connection()
        uri = '/batch_store/%s/%s' % (hostname, sourcetype)
        headers = {}
        conn.request('POST', uri, body=spool_file, headers=headers)
        resp = conn.getresponse()

        assert resp.status in (200,204), "Didn't get 200 from remote server!"

