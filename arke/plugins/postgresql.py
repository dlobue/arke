
import logging

import psycopg2

from arke.plugin import collect_plugin

class postgresql(collect_plugin):
    name = "postgresql"
    format = 'json'

    default_config = {'interval': 30,
                      'host': 'localhost',
                      'port': 5432,
                      'database': 'postgres',
                     }

    def activate(self):
        super(self.__class__, self).activate()
        self.connection = psycopg2.connect(host=self.get_setting('host'),
                                           port=self.get_setting('port', opt_type=int),
                                           user=self.get_setting('user'),
                                           password=self.get_setting('password'),
                                           database=self.get_setting('database'),
                                          )

    def run(self):
        cursor = self.connection.cursor()
        #raises OperationalError on slave
        cursor.execute('SELECT pg_current_xlog_location()')

        #returns none,none on solo and master
        cursor.execute('SELECT pg_last_xlog_receive_location(), pg_last_xlog_replay_location()')
        data = cursor.fetchone()
        cursor.close()

        return data



if __name__ == '__main__':
    from giblets import ComponentManager
    cm = ComponentManager()
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
        postgresql.default_config['host'] = host
        if port:
            postgresql.default_config['port'] = port

    data = postgresql(cm).run()
    from pprint import pprint
    pprint(data)

