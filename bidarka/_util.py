import os.path as osp
import os
import imp
from string import Template

from ._const import BIDARKA_CONFIG_PATH, RUN_SETTINGS


def script_path(key):
    path = osp.join(osp.dirname(__file__), key)
    return path


def read_template(key):
    path = script_path(key=key)
    with open(path, 'r') as f:
        src = f.read()
    return src


def apply_file_template(template, kv):
    with open(template, 'r') as f:
        src = Template(f.read())
        result = src.substitute(kv)
    return result


def template_transform(text, d, kv=None):
    t = Template(text)
    if kv is None:
        return t.substitute(d)
    else:
        return t.substitute(d, **kv)


def read_settings():
    path = os.getenv(BIDARKA_CONFIG_PATH, None)
    settings = _read_settings(path)
    return settings


def update_env(settings):
    for k, v in settings.items():
        os.environ[k] = v


def set_env_variables(env_variables: dict):
    for k, v in env_variables.items():
        os.environ[k] = v


def _read_settings(path=None):
    if path is None and osp.exists(RUN_SETTINGS):
        config = imp.load_source("config", RUN_SETTINGS)
    elif path is None:
        config = imp.load_source("config", osp.join(osp.dirname(__file__), RUN_SETTINGS))
    else:
        config = imp.load_source("config", osp.join(osp.dirname(path), RUN_SETTINGS))
    return config
