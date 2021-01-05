from _util import read_config, script_path
import subprocess
import logging
from string import Template
import os.path as osp
import tempfile

logging.basicConfig(format='%(message)s', level=logging.INFO)


def beeline(command, payload={}):
    config = read_config()
    beeline_config = config.BEELINE_CONFIG
    beeline_config.update(dict(command=command))
    src = _read_template().split()
    script = [_transform(t, beeline_config, payload) for t in src]
    logging.info(script)
    subprocess.check_call(script)


def hive_hql(path, payload={}):
    config = read_config()
    beeline_config = config.BEELINE_CONFIG
    dirpath = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain")
    script_text = _apply_file_template(path, payload)
    with tempfile.NamedTemporaryFile(suffix=".hql", dir=dirpath) as f:
        f.write(script_text)
        f.flush()
        command_line = "/usr/bin/beeline -u {} -f {}".format(beeline_config['beeline'], f.name)
        logging.info(command_line)
        subprocess.check_call(command_line.split())


def _transform(text, d, kv):
    t = Template(text)
    return t.substitute(d, **kv)


def _read_template():
    path = script_path("templates/beeline.template")
    with open(path, 'r') as f:
        src = f.read()
    return src


def _apply_file_template(template, kv):
    with open(template, 'r') as f:
        src = Template(f.read())
        result = src.substitute(kv)
    return result


if __name__ == '__main__':
    beeline("select indx from hcaadvaph_wk_playground.bidarka2_test limit 10;")
    path = script_path("scripts/hive_test.hql")
    hive_hql(path, payload=dict(db='hcaadvaph_wk_playground',tbl='bidarka2_test'))
