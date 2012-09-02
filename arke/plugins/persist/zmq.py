
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

try:
    import zmq.green as zmq
except ImportError:
    from gevent_zeromq import zmq

from gevent import sleep

from arke.errors import PersistError
from .base import ipersist


class zmq_backend(ipersist):
    def __init__(self, *args, **kwargs):
        super(zmq_backend, self).__init__(*args, **kwargs)
        self.host = self.config.get(self.section, 'host')
        self.port = None

        if self.config.has_option(self.section, 'port'):
            self.port = self.config.getint(self.section, 'port')
        if self.config.has_option('core', 'debug'):
            self.debug = self.config.getboolean('core', 'debug')
        else:
            self.debug = False

        self.context = zmq.Context()
        #self.socket = self.context.socket(zmq.REQ)
        #self.socket.connect('tcp://%s:%s' % (self.host, self.port))

    def get_connection(self):
        #return self.socket
        #TODO: socket pool
        socket = self.context.socket(zmq.REQ)
        socket.connect('tcp://%s:%s' % (self.host, self.port))
        return socket


    def batch_write(self, spool_file):
        spool_file.seek(0)
        conn = self.get_connection()
        conn.send_json(dict(action='persist',
                            data=spool_file.read()))

        receipt = conn.recv()

        while 1:
            conn.send_json(dict(action='check',
                                data=receipt))
            response = conn.recv_json()
            if response['status']:
                break
            elif response['status'] is None:
                raise PersistError
            sleep(5)

