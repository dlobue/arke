
import logging
from datetime import datetime

import psycopg2

from arke.plugin import collect_plugin, config, timer2
from arke.util import partial

class postgresql(collect_plugin):
    name = "postgresql"
    format = 'json'

    default_config = {'interval': 30,
                      'host': 'localhost',
                      'port': 5432,
                      'database': 'postgres',
                      'user': None,
                      'password': None,
                     }


    def activate(self):
        self.is_activated = True
        self.connection = psycopg2.connect(host=self.get_setting('host'),
                                           port=self.get_setting('port', opt_type=int),
                                           user=self.get_setting('user'),
                                           password=self.get_setting('password'),
                                           database=self.get_setting('database'),
                                          )

        self.schedule_run()

    def schedule_run(self):
        next_run_kwargs = {'microsecond': 0}
        interval_secs = self.get_setting('interval', opt_type=int)
        now = datetime.now()
        next_run_kwargs['second'] = (now.second - (now.second % interval_secs)) + interval_secs
        if next_run_kwargs['second'] >= 60:
            minutes = next_run_kwargs['second'] / 60
            next_run_kwargs['minute'] = now.minute + minutes
            next_run_kwargs['second'] -= 60 * minutes

        next_run = now.replace(**next_run_kwargs)

        self._timer = timer2.apply_at(next_run, self.queue_run)

    def run(self):
        return {'repl': self._repl(),
               }

    def _repl(self):
        data = {'cur': None,
                'rec': None,
                'rep': None,
               }

        cursor = self.connection.cursor()
        try:
            #raises OperationalError on slave
            cursor.execute('SELECT pg_current_xlog_location()')
            data['cur'] = cursor.fetchone()[0]
        except psycopg2.OperationalError:
            #returns none,none on solo and master
            cursor.execute('SELECT pg_last_xlog_receive_location(), pg_last_xlog_replay_location()')
            data['rec'], data['rep'] = cursor.fetchone()
        finally:
            cursor.close()

        return data

    def __call__(self, data):
        return self.serialize(data)

    def queue_run(self):
        data = self.run()
        config.queue_run(item=partial(self, data))


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

