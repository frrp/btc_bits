def init_config(config_module, default_config_module=None):
    '''
        This will import modules config_default and config and move their variables
        into current module (variables in config have higher priority than config_default).
        Thanks to this, you can import config anywhere in the application and you'll get
        actual application config.

        This config is related to server side. You don't need config.py if you
        want to use client part only.
    '''

    def read_values(cfg):
        for varname in cfg.__dict__.keys():
            if varname.startswith('__'):
                continue

            value = getattr(cfg, varname)
            yield (varname, value)

    import sys
    module = sys.modules[__name__]

    # Load the default configuration
    if default_config_module is not None:
        for name, value in read_values(default_config_module):
            module.__dict__[name] = value

    changes = {}
    if config_module is not None:
        for name, value in read_values(config_module):
            if value != module.__dict__.get(name, None):
                changes[name] = value
            module.__dict__[name] = value

    if module.__dict__['DEBUG'] and changes:
        print "----------------"
        print "Custom settings:"
        for k, v in changes.items():
            if 'passw' in k.lower():
                print k, ": ********"
            else:
                print k, ":", v
        print "----------------"
