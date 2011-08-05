
import logging

logger = logging.getLogger(__name__)

from circuits import Component, Event
from circuits.web.client import Client, Request
from bson import BSON

from arke.spool import Spooler

class Persist(Component):

    def started(self, *args, **kwargs):
        self.spool = Spooler().register(self)
        b = self.root.config.get('core', 'persist_backend')
        b = b.lower()
        h = self.root.config.get('backend', 'host')
        p = self.root.config.get('backend', 'port')
        if b in ('http', 'https'):
            self.backend = HTTPClient(host=h, port=p, scheme=b).register(self)
        else:
            logger.error("Invalid backend given: %s" % b)
            raise SystemExit

    def serialize(self, data):
        return BSON.encode(data)


class HTTPClient(Client):
    def __init__(self, host, port, scheme, channel= None):
        url = '%s://%s:%s/' % (scheme, host, port)
        if channel is None:
            channel = self.channel
        super(HTTPClient, self).__init__(url, channel=channel)

