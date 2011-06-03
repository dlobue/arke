
import logging

import pymongo

from arke.plugin import collect_plugin

class mongodb(collect_plugin):
    name = "mongodb"
    serialize = 'json'

    def run(self):
        logging.info("collecting %s data" % self.name)
        return dict(
            server_status=self._db.command('serverStatus'),
            repl_status=self._db.command('replSetGetStatus')
        )

