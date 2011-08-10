
import logging

logger = logging.getLogger(__name__)

import psutil
from psutil._pslinux import wrap_exceptions

from arke.collect import Collect

class ExProcess(psutil.Process):
    @property
    def _process_name(self):
        return self._platform_impl._process_name

    @property
    @wrap_exceptions
    def oom_score(self):
        with open('/proc/%i/oom_score' % self.pid, 'r') as f:
            return int(f.readline().strip())


class system(Collect):
    default_config = {'interval': 30,
                     }

    def collect(self):
        return dict(
            cpu_times=psutil.cpu_times()._asdict(),
            mem=dict(
                total_phymem=psutil.TOTAL_PHYMEM,
                avail_phymem=psutil.avail_phymem(),
                avail_virtmem=psutil.avail_virtmem(),
                cached_phymem=psutil.cached_phymem(),
                phymem_buffers=psutil.phymem_buffers(),
                total_virtmem=psutil.total_virtmem(),
                used_phymem=psutil.used_phymem(),
                used_virtmem=psutil.used_virtmem(),
            ),
            processes=list(self._processes()),
            net=dict(
                ifaces=self._net_dev(),
                proto=self._net_proto()
            ),
            io=self._io_stats(),
            fs=dict(self._fs_usage()),
            fh=self._file_handles(),
        )

    def _processes(self):
        for pid in psutil.get_pid_list():
            try:
                process = ExProcess(pid)
                if not process.cmdline:
                    continue
                yield dict(
                    name=process.name,
                    cmdline=' '.join(process.cmdline),
                    status=str(process.status),
                    ppid=process.ppid,
                    pid=process.pid,
                    cpu_times=process.get_cpu_times()._asdict(),
                    io_counters=process.get_io_counters()._asdict(),
                    memory=process.get_memory_info()._asdict(),
                    oom_score=process.oom_score,
                    num_threads=process.get_num_threads(),
                    connections=[c._asdict() for c in process.get_connections()],
                    open_files=[f.path for f in process.get_open_files()],
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
                if ':' not in line: continue
                iface, stats = line.split(":")
                ifaceData = map(int, stats.split())
                if not any(ifaceData):
                    continue
                ifaces[iface.strip()] = dict(zip(cols, ifaceData))

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
        for partition in psutil.disk_partitions():
            usage = psutil.disk_usage(partition.mountpoint)._asdict()
            usage['filesystem'] = partition.device
            yield (partition.mountpoint, usage)


            
if __name__ == '__main__':
    from pprint import pprint

    pprint(system(None,None,None,None).collect())

