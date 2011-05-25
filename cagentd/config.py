
from weakref import ref

class NoSettings(Exception): pass

main_object = None
def set_main_object(mo):
    global main_object
    main_object = ref(mo)

def get_config():
    global main_object
    if not main_object():
        raise NoSettings
    return main_object().config_parser

