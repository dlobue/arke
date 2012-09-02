
from __future__ import absolute_import

from os.path import getsize
import logging

logger = logging.getLogger(__name__)

try:
    import zmq.green as zmq
except ImportError:
    from gevent_zeromq import zmq

from gevent import sleep, getcurrent, Timeout
from gevent.coros import BoundedSemaphore

from arke.errors import PersistError
from .base import ipersist

ERROR = '\xff'
SUCCESS = '\x00'
NOTDONE = '\x01'

class zmq_backend(ipersist):
    def __init__(self, *args, **kwargs):
        super(zmq_backend, self).__init__(*args, **kwargs)
        self.host = self.config.get(self.section, 'host')
        self.port = 64000

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
        #TODO: figure out how zmq handles DNS records with multiple IPs
        context = self.context.instance()
        req = context.socket(zmq.REQ)
        router = context.socket(zmq.ROUTER)
        req.setsockopt(zmq.LINGER, 0)
        router.setsockopt(zmq.LINGER, 0)
        req.connect('tcp://%s:%s' % (self.host, self.port))
        router.connect('tcp://%s:%s' % (self.host, self.port))
        return req,router


    def batch_write(self, spool_file):
        spool_file.seek(0)
        req,router = self.get_connection()
        buf = bytearray(getsize(spool_file.name))
        spool_file.readinto(buf)
        req.send_multipart(('persist', zmq.Message(buf)), copy=False)

        try:
            with Timeout(60, PersistError):
                while 1:
                    try:
                        address, receipt = req.recv_multipart(zmq.NOBLOCK)
                        break
                    except zmq.ZMQError, e:
                        if e.strerror != 'Resource temporarily unavailable':
                            raise e
                        sleep(.5)
        except:
            req.close()
            router.close()
            raise
        req.close()

        check_msg = (address, '', 'check', receipt)

        while 1:
            router.send_multipart(check_msg)

            response = None
            with Timeout(5, False):
                while 1:
                    try:
                        _,_,response = router.recv_multipart(zmq.NOBLOCK)
                        break
                    except zmq.ZMQError, e:
                        if e.strerror != 'Resource temporarily unavailable':
                            raise e
                        sleep(.5)

            if response is None:
                logger.warn("Didn't get any response to check request. will try again.")
            elif response == SUCCESS:
                router.close()
                return
            elif response == ERROR:
                router.close()
                raise PersistError
            elif response == NOTDONE:
                logger.debug('Server %s says %s is not done yet' % (address, receipt))
            sleep(5)

