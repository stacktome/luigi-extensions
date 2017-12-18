import configparser


def run_once(method):
    """A decorator that runs a method only once."""

    attrname = "_%s_once_result" % id(method)

    def decorated(self, *args, **kwargs):
        try:
            return getattr(self, attrname)
        except AttributeError:
            setattr(self, attrname, method(self, *args, **kwargs))
            return getattr(self, attrname)
    return decorated


def run_once_property(method):
    return property(run_once(method))


def load_configuration(param_file, sect_name='general'):
    config = configparser.ConfigParser()
    config.read(param_file)
    options = config.options(sect_name)
    params = {}
    for opt in options:
        params[opt] = config.get(sect_name, opt)
    convert_to_number(params)
    return params


def convert_to_number(params):
    for k, v in params.items():
        try:
            params[k] = float(v)
            if float(v) == int(v):
                params[k] = int(v)
        except ValueError:
            if v.lower() == 'true': params[k] = True
            if v.lower() == 'false': params[k] = False
            pass
