
from time import time

from circuits.core import BaseComponent


class NormalizedTimer(BaseComponent):
    def __init__(self, s, e, c="timer", t=None, persist=False, normalize=False):
        self.normalize = normalize
        super(NormalizedTimer, self).__init__(s=s, e=e, c=c, t=t, persist=persist)

    def reset(self):
        t = time()
        s = self.s
        if self.normalize:
            s = (s - (t % s))

        self._eTime = t + s

