
class ipersist(object):
    def __init__(self, config_parser):
        self.config = config_parser
        self.section = self.__class__.__name__.replace('_backend', '')

    def write(self, sourcetype, timestamp, data, hsh, hostname):
        """
        If the write fails (tbd by implementation), return False so the
        runner knows to reschedule.
        Otherwise return True
        """
        raise NotImplemented

