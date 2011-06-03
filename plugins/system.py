
import logging
from subprocess import Popen, PIPE
from threading import Timer

import psutil
from psutil._pslinux import wrap_exceptions

from arke.plugin import collect_plugin

class ExProcess(psutil.Process):
    @property
    def _process_name(self):
        return self._platform_impl._process_name

    @property
    @wrap_exceptions
    def oom_score(self):
        with open('/proc/%i/oom_score' % self.pid, 'r') as f:
            return int(f.readline().strip())


class system(collect_plugin):
    name = "system"
    serialize = 'json'
    default_config = {'interval': 30,
                      'io_stats': None
                     }

    def run(self):
        logging.info("collecting %s data" % self.name)
        return dict(
            cpu_times=psutil.cpu_times()._asdict(),
            memory=dict(
                total_phymem=psutil.TOTAL_PHYMEM,
                avail_phymem=psutil.avail_phymem(),
                avail_virtmem=psutil.avail_virtmem(),
                cached_phymem=psutil.cached_phymem(),
                phymem_buffers=psutil.phymem_buffers(),
                total_virtmem=psutil.total_virtmem(),
                used_phymem=psutil.used_phymem(),
                used_virtmem=psutil.used_virtmem(),
            ),
            processes=dict(self._processes()),
            net=dict(
                ifaces=self._net_dev(),
                proto=self._net_proto()
            ),
            io_counters=self._io_stats(),
            fs=self._fs_usage(),
            fh=self._file_handles(),
        )

    def _processes(self):
        for pid in psutil.get_pid_list():
            try:
                process = ExProcess(pid)
                yield (process.pid,
                        dict(
                            name=process.name,
                            cmdline=' '.join(process.cmdline),
                            status=str(process.status),
                            ppid=process.ppid,
                            cpu_times=process.get_cpu_times()._asdict(),
                            io_counters=process.get_io_counters()._asdict(),
                            memory=process.get_memory_info()._asdict(),
                            oom_score=process.oom_score,
                            num_threads=process.get_num_threads(),
                            connections=[c._asdict() for c in process.get_connections()],
                            open_files=[f.path for f in process.get_open_files()],
                        )
                      )
            except psutil.NoSuchProcess:
                continue

    def _file_handles(self):
        with open('/proc/sys/fs/file-nr', 'r') as f:
            o,f,m = map(int, f.readline().split())
        return dict(
            open=o,
            free=f,
            max=m
        )

    def _net_proto(self):
        protocols = {}
        def _parse(fn):
            with open(fn, 'r') as f:
                for line in f:
                    proto, cols = line.split(':')
                    cols = cols.split()
                    nproto, data = f.next().split(':')
                    assert proto == nproto, "the format of %s has changed!" % fn
                    proto_data = dict(zip(cols, map(int, data.split())))

                    protocols[proto] = proto_data

        _parse('/proc/net/snmp')
        _parse('/proc/net/netstat')
        return protocols


    def _net_dev(self):
        ifaces = {}
        with open('/proc/net/dev', 'r') as f:

            f.readline()
            columnLine = f.readline()
            _, receiveCols , transmitCols = columnLine.split("|")
            receiveCols = map(lambda a:"recv_"+a, receiveCols.split())
            transmitCols = map(lambda a:"trans_"+a, transmitCols.split())

            cols = receiveCols+transmitCols

            for line in f:
                #TODO: get config data and determine if iface should be skipped
                #XXX: or detect if all values are 0 and if so, skip?
                if ':' not in line: continue
                iface, stats = line.split(":")
                ifaceData = map(int, stats.split())
                ifaceData = dict(zip(cols, map(int, stats.split())))
                ifaces[iface.strip()] = ifaceData

        return ifaces

    def _io_stats(self):
        cols = ('read_count',
                'reads_merged',
                'read_sectors',
                'reading_ms',
                'write_count',
                'writes_merged',
                'write_sectors',
                'writing_ms',
                'io_run',
                'io_rms',
                'io_twms')

        results = {}
        with open('/proc/diskstats', 'r') as f:
            for line in f:
                data = line.split()[2:]
                disk = data.pop(0)
                if disk.startswith('ram') or disk.startswith('loop'):
                    continue
                results[disk] = dict(zip(cols, map(int, data)))

        return results

    def _fs_usage(self):
        def numbify(x):
            try:
                return int(x)
            except ValueError:
                return x

        cmd = ['df', '-x', 'tmpfs', '-x', 'devtmpfs', '-x', 'debugfs', '--block-size=1']
        process = Popen(cmd, stdout=PIPE)
        timer = Timer(2, process.kill)
        timed_out = True
        timer.start()
        output, unused_err = process.communicate()
        retcode = process.poll()
        if timer.is_alive():
            timer.cancel()
            timed_out = False

        if retcode:
            logging.error(("Attempt to get output of df failed. Exit code: %i.\n"
                           "cmd: %s\nstdout: %s\nstderr: %s\ntimed out: %s") % \
                         (retcode, cmd.join(' '), output, unused_err, timed_out))
            return None

        results = {}
        cols = ('total', 'used', 'avail', 'percent', 'mount')
        output = output.replace('%','').splitlines()
        for line in output[1:]:
            data = line.split()
            fs = data.pop(0)
            results[fs] = dict(zip(cols, map(numbify, data)))

        return results
            
if __name__ == '__main__':
    from pprint import pprint
    pprint(system().run())

