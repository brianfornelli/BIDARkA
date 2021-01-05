import os.path as osp
import os
import imp
from _credentials import get_password, get_user


def script_path(key):
    path = osp.join(osp.dirname(__file__), key)
    return path


def read_config():
    path = os.getenv("BIDARKA_CONFIG_PATH", None)
    return _read_config(path)


def _read_config(path=None):
    config_file = "config.eio"
    if path is None and osp.exists(config_file):
        config = imp.load_source("config", config_file)
    elif path is None:
        config = imp.load_source("config", osp.join(osp.dirname(__file__), config_file))
    else:
        config = imp.load_source("config", osp.join(osp.dirname(path), config_file))
    config.USERNAME = get_user()
    config.PASSWORD = get_password()
    return config


class Command(dict):
    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.iteritems():
                    self[k] = v
        if kwargs:
            for k, v in kwargs.iteritems():
                self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Command, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Command, self).__delitem__(key)
        del self.__dict__[key]
