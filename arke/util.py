
from time import time, mktime
from datetime import datetime
from threading import Lock

from gevent import sleep, getcurrent

from circuits.core import BaseComponent
from circuits.core.timers import Timer


class NormalizedTimer(Timer):
    def __init__(self, s, e, c="timer", t=None, persist=False, normalize=False):
        self.normalize = normalize
        super(NormalizedTimer, self).__init__(s=s, e=e, c=c, t=t, persist=persist)

    def reset(self):
        t = time()
        s = self.s
        if self.normalize:
            s = (s - (t % s))

        self._eTime = t + s



class GreenTimer(BaseComponent):
    """Timer(s, e, c, t, persist) -> new timer component

    Creates a new timer object which when triggered
    will push the given event onto the event queue.

    s := no. of seconds to delay
    e := event to be fired
    c := channel to fire event to
    t := target to fire event to

    persist := Sets this timer as persistent if True.
    """

    def __init__(self, s, e, c="timer", t=None, persist=False, normalize=False):
        super(GreenTimer, self).__init__()

        if isinstance(s, datetime):
            self.s = mktime(s.timetuple()) - time()
        else:
            self.s = s

        self.e = e
        self.c = c
        self.t = t
        self.persist = persist
        self.normalize = normalize
        
        self._timer = None
        self._lock = Lock()

    def cancel(self):
        with self._lock:
            if self._timer is not None:
                self._timer.kill()
                self._timer = None

    def reset(self):
        t = time()
        s = self.s
        if self.normalize:
            s = (s - (t % s))

        with self._lock:
            self._timer = getcurrent()
        sleep(s)
        with self._lock:
            self._timer = None

        self.fire(self.e, self.c, self.t)

        if self.persist:
            self.reset()
        else:
            self.unregister()


    def registered(self, component, manager):
        if manager is not self and manager is self.manager \
           and component is self:
            self.reset()

    def unregistered(self, component, manager):
        if manager is not self and manager is self.manager \
           and component is self:
            self.cancel()

