from ._util import apply_file_template
from ._credentials import get_password, get_user
import subprocess
import logging


logging.basicConfig(format='%(message)s', level=logging.INFO)


def bteq(script_path, payload={}):
    user = get_user()
    password = get_password()
    bteq_config = dict(USERNAME=user, PASSWORD=password)
    payload.update(bteq_config)
    sql = apply_file_template(script_path, payload)
    proc = subprocess.Popen(['bteq'], stdin=subprocess.PIPE)
    proc.communicate(sql.encode('utf-8'))
    if proc.returncode:
        raise subprocess.CalledProcessError(returncode=proc.returncode,
                                            cmd="bteq(script_path='{}')".format(script_path))
