
import logging

logger = logging.getLogger(__name__)

from arke.collect import Collect

class queue_monitor(Collect):
    default_config = {'interval': 60,
                     }

    def gather_data(self):
        logger.info("persist_queue: %i, collect_pool: %i" % (self.persist_queue.qsize(), len(self._pool)))

