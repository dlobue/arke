
class partial(object):
    def __init__(self, obj, *args, **kwargs):
        self.__obj = obj
        self.__args = args
        self.__kwargs = kwargs

    def __getattr__(self, attr):
        try:
            return getattr(self.__obj, attr)
        except AttributeError:
            raise AttributeError

    def __call__(self):
        return self.__obj(*self.__args, **self.__kwargs)

