
import json
from time import time
import logging
from socket import error, timeout

from bson import json_util, BSON
#from giblets import Component, ComponentManager, ExtensionPoint, implements
#from giblets.policy import Blacklist
#from giblets.search import find_plugins_by_entry_point, find_plugins_in_path
import timer2
import boto
from circuits import Component, Event

import config
#from arke.interfaces import icollecter
from arke.util import partial, get_credentials


def get_plugin_manager(search_paths=None):
    mgr = PluginManager()
    blacklist = Blacklist()
    blacklist.disable_component(collect_plugin)
    blacklist.disable_component(multi_collect_plugin)
    mgr.restrict(blacklist)
    find_plugins_by_entry_point('arke_plugins')
    if search_paths:
        find_plugins_in_path(search_paths)
    return mgr

#class PluginManager(ComponentManager):
    #collection_plugins = ExtensionPoint(icollecter)
    #compmgr = property(lambda self: self)


class collect_plugin(Component):
    #implements(icollecter)
    format = None
    custom_schema = False
    timestamp_as_id = False

    default_config = {'interval': 30}

    @property
    def config(self):
        try:
            return config.get_config()
        except config.NoSettings:
            return None

    def get_setting(self, setting, fallback=None, opt_type=None):
        if self.config and self.config.has_option(self.section, setting):
            try:
                getter = {int: self.config.getint,
                          float: self.config.getfloat,
                          bool: self.config.getboolean,
                         }[opt_type]
            except KeyError:
                getter = self.config.get
            return getter(self.section, setting)
        else:
            val = self.default_config.get(setting, fallback)
            if opt_type in (None, bool) or isinstance(val, opt_type):
                return val
            try:
                val = opt_type(val)
            except ValueError:
                return val

    def __init__(self):
        super(collect_plugin, self).__init__()
        self.is_activated = False
        self.channel = self.name
        #self.name = self.__class__.__name__
        self.section = 'plugin:%s' % self.name
        self._timer = None
        assert 'interval' in self.default_config, (
            "Missing default interval value for %s plugin" % self.name)


    @property
    def enabled(self):
        return self.get_setting('enabled', False, opt_type=bool)

    def activate(self):
        self.is_activated = True

        secs = self.get_setting('interval', opt_type=int)
        msecs = secs * 1000 #convert to miliseconds
        self._timer = timer2.apply_interval(msecs, self.queue_run)

        self.queue_run()

    def deactivate(self):
        self.is_activated = False
        if self._timer:
            self._timer.cancel()

    def queue_run(self):
        config.queue_run(item=self)

    def __call__(self):
        return self.serialize(self.collect())

    def serialize(self, data):
        if not self.format:
            return data

        if self.format.lower() == "json":
            return json.dumps(data)
        elif self.format.lower() == "bson":
            return BSON.encode(data)
        elif self.format.lower() == "extjson":
            return json.dumps(data, default=json_util.default)

    #if __name__ == '__main__':
        #def started(self):
            #self.fire(Event(), 'collect')

    #def collect(self):
        #raise NotImplemented

    def gather_data(self):
        timestamp = time()
        sourcetype = self.name
        #key needs to be generated before we normalize
        key = '%f%s' % (timestamp, sourcetype)
        extra = {}

        if self.timestamp_as_id:
            #if the timestamp is going to be used as the id, then
            #that means we're going to group multiple results into
            #the same document. round the timestamp in order to ensure
            #we catch everything.
            offset = timestamp % self.get_setting('interval')
            timestamp = timestamp - offset
            extra['timestamp_as_id'] = True

        if self.custom_schema:
            extra['custom_schema'] = True

        if self.format:
            extra['ctype'] = self.format.lower()

        logging.debug("gathering data for %s sourcetype" % sourcetype)
        try:
            data = self.serialize(self.collect())
            #data = plugin()
        except Exception, e:
            logging.exception("error occurred while gathering data for sourcetype %s" % sourcetype)
            raise e


        #TODO: put stuff in spool
        #XXX: use spool as queue

        spool[key] = (sourcetype, timestamp, data, extra)
        
        self.persist_queue.put(key)


class multi_collect_plugin(collect_plugin):
    format = 'json'
    hostname = None
    custom_schema = True
    timestamp_as_id = True
    default_config = {'interval': 10,
                      'region': None,
                     }

    def queue_run(self):
        if not self.hostname:
            self.hostname = self.config.get('core', 'hostname')

        if not hasattr(self, 'sdb_domain'):
            sdb = boto.connect_sdb(*get_credentials())
            log = logging.getLogger('boto')
            log.setLevel(logging.INFO)
            self.sdb_domain = sdb.get_domain('chef')

        query = 'select fqdn,ec2_public_hostname from chef where fqdn is not null'
        region = self.get_setting('region')
        if region:
            query += " and ec2_region = '%s'" % region
        logging.debug('looking for peers with the query: %s' % query)
        servers = self.sdb_domain.select(query)
        for server in servers:
            if server['fqdn'] == self.hostname:
                continue
            config.queue_run(item=partial(self, server))

    def __call__(self, server):
        return self.serialize(self.collect(server))

