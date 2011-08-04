
import json
from time import time
import logging
from glob import glob
import imp
import os
import sys
from functools import partial
from inspect import getmembers, getmodule, isclass
from pprint import pprint

from circuits import Component, Event, Timer
from circuits.core.handlers import HandlerMetaClass, handler
from circuits import Manager, Debugger
from circuits.app.config import Config as _Config, Load

from arke.plugin import plugin_manager, collect_plugin

#class Config(_Config):
    #@handler('getboolean')
    #def getboolean(self, *args, **kwargs):
        #return super(Config, self).getboolean(*args, **kwargs)

Config = HandlerMetaClass('Config', (_Config,), _Config.__dict__.copy())

mylogger = logging.getLogger()
mylogger.setLevel(logging.DEBUG)
h = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
h.setFormatter(formatter)
mylogger.addHandler(h)
m = Manager()
logging.debug('manager init')
m += Debugger()
logging.debug('debugger init + register')
#l = Logger()
#Config.__dict__['__metaclass__'] = HandlerMetaClass
#Config = HandlerMetaClass('Config', (_Config,), {})
c = Config(sys.argv[1])
print("\n\n")
print(c.name)
pprint(c._handlers)
print("\n\n")
#m += l
m += c
m.fire(Load(), target='config')
#while m: m.flush()
logging.debug('config init + register')
p = plugin_manager(paths=sys.argv[2])
logging.debug('pm init')
m += p
logging.debug('pm register')
#while m: m.flush()
#(Manager() + Logger() + Config(sys.argv[1]) + plugin_manager(paths=sys.argv[2])).run()
#while m: m.flush()

m.fire(Event(collect_plugin), 'load', target='plugin_manager')
#p.load(collect_plugin)
logging.debug('pm load')
m.run()

