
from ConfigParser import SafeConfigParser
from os.path import expanduser

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

def get_credentials():
    confp = SafeConfigParser()
    confp.read(expanduser('~/.s3cfg'))
    return (confp.get('default', 'access_key'),
                     confp.get('default', 'secret_key'))

