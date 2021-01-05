from _util import read_config, script_path
import subprocess
import logging
from string import Template
import tempfile
import os.path as osp

logging.basicConfig(format='%(message)s', level=logging.INFO)


def renew_kerberos():
    dirpath = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain")
    sql = _read_script()
    with tempfile.NamedTemporaryFile(suffix=".sh", dir=dirpath) as f:
        f.write(sql)
        f.flush()
        logging.info("{}".format(f.name))
        subprocess.call(["dos2unix", f.name])
        subprocess.call(["/bin/bash", f.name])


def _read_script():
    config = read_config()
    payload = dict(PASSWORD=config.PASSWORD)
    path = script_path("templates/kerberos.template")
    with open(path, 'r') as f:
        src = Template(f.read())
        sql = src.substitute(payload)
    return sql


if __name__ == '__main__':
    renew_kerberos()
