from _util import read_config, script_path
import subprocess
import logging
from string import Template

logging.basicConfig(format='%(message)s', level=logging.INFO)


def bteq(script_path, payload={}):
    config = read_config()
    bteq_config = config.BTEQ_CONFIG
    bteq_config.update(dict(USERNAME=config.USERNAME, PASSWORD=config.PASSWORD))
    with open(script_path, 'r') as f:
        src = Template(f.read())
        sql = src.substitute(bteq_config, **payload)
    proc = subprocess.Popen(['bteq'], stdin=subprocess.PIPE)
    proc.communicate(sql)
    if proc.returncode:
        raise subprocess.CalledProcessError(returncode=proc.returncode,
                                            cmd="bteq(script_path='{}')".format(script_path))


if __name__ == '__main__':
    test_path = script_path("scripts/bteq_test.bteq")
    bteq(test_path)
