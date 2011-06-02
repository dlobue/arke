
import logging

import psutil

from arke.plugin import collect_plugin

class system(collect_plugin):
    name = "system"
    serialize = 'json'

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
            processes=dict(self._processes())
        )

    def _processes(self):
        for process in psutil.process_iter():
            try:
                yield (process.pid,
                        dict(
                            name=process.name,
                            cmdline=' '.join(process.cmdline),
                            status=str(process.status),
                            ppid=process.ppid,
                            cpu_times=process.get_cpu_times()._asdict(),
                            io_counters=process.get_io_counters()._asdict(),
                            memory=process.get_memory_info()._asdict(),
                            num_threads=process.get_num_threads(),
                            connections=[c._asdict() for c in process.get_connections()],
                            open_files=[f.path for f in process.get_open_files()],
                        )
                      )
            except psutil.NoSuchProcess:
                continue

