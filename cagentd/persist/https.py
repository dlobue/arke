
import httplib

from .http import http_backend

class https_backend(http_backend):
    def get_connection(self):
        return httplib.HTTPSConnection(self.host)

