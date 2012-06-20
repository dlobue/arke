
from functools import wraps
from gevent.pool import Pool

def parent_too(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        self._parent._semaphore.acquire()
        try:
            greenlet = f(self, *args, **kwargs)
            self._parent.add(greenlet)
        except:
            self._parent._semaphore.release()
            raise
        return greenlet
    return wrapper


class KiddiePool(Pool):
    def __init__(self, parent, size=None, greenlet_class=None):
        super(KiddiePool, self).__init__(size=size, greenlet_class=greenlet_class)
        self._parent = parent

    spawn = parent_too(Pool.spawn)
    spawn_link = parent_too(Pool.spawn_link)
    spawn_link_value = parent_too(Pool.spawn_link_value)
    spawn_link_exception = parent_too(Pool.spawn_link_exception)

    def start(self, greenlet):
        self._parent._semaphore.acquire()
        try:
            self._parent.add(greenlet)
        except:
            self._parent._semaphore.release()
            raise
        return super(KiddiePool, self).start(greenlet)

    def wait_available(self):
        self._parent.wait_available()
        return super(KiddiePool, self).wait_available()

    #def free_count(self):
        #return min(self._parent.free_count(),
                   #super(KiddiePool, self).free_count())


if __name__ == '__main__':
    from gevent import sleep
    from datetime import datetime
    from random import randint
    parent = Pool(2)
    child = KiddiePool(parent, 5)

    def myfunc(count):
        print("%s, [%s] p:%s|%s c:%s|%s" % (datetime.now(), count, len(parent), parent.free_count(), len(child), child.free_count()))
        sleep(randint(1, 5))
        print("%s, [%s] p:%s|%s c:%s|%s" % (datetime.now(), count, len(parent), parent.free_count(), len(child), child.free_count()))

    for i in xrange(20):
        child.spawn(myfunc, i)

    parent.join()




