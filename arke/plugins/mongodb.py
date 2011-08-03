
import logging

import pymongo

from arke.plugin import collect_plugin

class mongodb(collect_plugin):
    format = 'extjson'

    default_config = {'interval': 30,
                      'host': 'localhost',
                      'port': 27017,
                     }

    def collect(self):
        connection = pymongo.Connection(self.get_setting('host'),
                                        self.get_setting('port', opt_type=int),
                                        slave_okay=True,
                                       )
        db = connection.admin

        try:
            repl_status = db.command('replSetGetStatus')
        except pymongo.errors.OperationFailure:
            logging.debug("Mongodb server is not part of a replica set")
            repl_status = None

        try:
            return dict(
                server_status=db.command('serverStatus'),
                repl_status=repl_status,
                col_stats=dict(self._coll_stats(connection))
            )
        except Exception:
            logging.exception("Error while collecting mongodb server status")

    def _coll_stats(self, connection):
        for db_name in connection.database_names():
            if db_name in ('admin', 'local'):
                continue
            colls = {}
            db = connection[db_name]
            for coll_name in db.collection_names():
                if coll_name in ('system.indexes',):
                    continue
                colls[coll_name] = db.command('collstats', coll_name)

            if colls:
                yield (db_name, colls)


if __name__ == '__main__':
    #from giblets import ComponentManager
    #cm = ComponentManager()
    from sys import argv
    try:
        host = argv[1]
    except IndexError:
        host = None
    else:
        try:
            port = argv[2]
        except IndexError:
            port = None

    if host:
        mongodb.default_config['host'] = host
        if port:
            mongodb.default_config['port'] = port

    data = mongodb().collect()
    from pprint import pprint
    pprint(data)

