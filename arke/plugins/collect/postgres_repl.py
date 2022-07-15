
import logging
from time import sleep

logger = logging.getLogger(__name__)

import psycopg2

from arke.collect import Collect

MAX_ATTEMPTS = 5

class NoConnection(Exception): pass

class postgres_repl(Collect):
    default_config = {'interval': 30,
                      'hosts': 'localhost',
                      'port': 5432,
                      'database': 'postgres',
                      'user': None,
                      'password': None,
                     }


    def iter_connections(self):
        if not hasattr(self, 'connections'):
            self.connections = {}
        conns = self.connections

        hosts = self.get_setting('hosts')
        hosts = hosts.replace(',', ' ')
        hosts = hosts.split()

        default_port = self.get_setting('port', opt_type=int)

        for host in conns.copy():
            if host not in hosts:
                conns.pop(host)

        for host in hosts:
            if ':' in host:
                hoststr,port = host.split(':')
                port = int(port)
                if port == default_port:
                    host = hoststr
            else:
                hoststr = host
                port = None

            if host in conns:
                try:
                    conns[host].set_session(readonly=True, autocommit=True)
                except psycopg2.OperationalError:
                    #lost the connection, so remove it from pool
                    conns.pop(host)

            if host not in conns:
                connect_params = dict(
                    host=hoststr,
                    port=port or default_port,
                    user=self.get_setting('user'),
                    database=self.get_setting('database'),
                )
                if self.get_setting('password') is not None:
                    connect_params['password'] = self.get_setting('password')
                conns[host] = psycopg2.connect(
                    **connect_params
                )
                conns[host].set_session(readonly=True, autocommit=True)
            yield host, conns[host]

    def collect(self, attempt=0):
        result = {}
        for host,connection in self.iter_connections():
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT pg_current_xlog_location()')
                result['master'] = cursor.fetchone()[0]
            except psycopg2.OperationalError:
                if connection.closed:
                    result = None
                    break
                try:
                    cursor.execute('SELECT pg_last_xlog_receive_location(), pg_last_xlog_replay_location()')
                except psycopg2.OperationalError:
                    if connection.closed:
                        result = None
                        break
                slaves = result.setdefault('slaves', [])
                d = {'host': host}
                d['r'], d['p'] = cursor.fetchone()
                slaves.append(d)
            finally:
                cursor.close()

        if result is None:
            if attempt > MAX_ATTEMPTS:
                logger.error("Connection to postgres servers was interrupted more than %i times - going to bail.")
                raise NoConnection
            else:
                logger.error(
                    f"Lost connection to postgres server {host} while retrieving WAL location. Going to try again."
                )

                sleep(5)
                return self.run(attempt+1)

        return result


if __name__ == '__main__':
    from sys import argv
    try:
        user = argv[1]
    except IndexError:
        user = None
    else:
        try:
            hosts = argv[2]
        except IndexError:
            hosts = None
        else:
            try:
                port = argv[3]
            except IndexError:
                port = None

    if user:
        postgres_repl.default_config['user'] = user

    if hosts:
        postgres_repl.default_config['hosts'] = hosts
        if port:
            postgres_repl.default_config['port'] = port

    data = postgres_repl().collect()
    from pprint import pprint
    pprint(data)

