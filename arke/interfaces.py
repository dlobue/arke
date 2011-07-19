
from giblets import ExtensionInterface, Attribute


class icollecter(ExtensionInterface):
    default_config = Attribute('default configuration variables that can be overriden in config file.')
    format = Attribute('what format should be used to serialze the result')
    custom_schema = Attribute('use a custom data storage schema?')
    timestamp_as_id = Attribute('use timestamp as id?')
    priority = Attribute("plugin's priority in the run queue.")
    is_activated = Attribute('is the extension activated?')
    enabled = Attribute('is this extension enabled?')
    def activate():
        'activate the plugin'
    def deactivate():
        'deactivate the plugin'
    def run():
        'the thing to do to get the info'

