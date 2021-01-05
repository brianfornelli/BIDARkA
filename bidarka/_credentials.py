import logging
import getpass
import os
import os.path as osp
import base64
from ._const import BIDARKA_DIR, ENC_FILE


logging.basicConfig(format='%(message)s', level=logging.INFO)


def prompt_password() -> None:
    p = getpass.getpass(prompt="Password:")
    _store_pass(p)


def get_password() -> str:
    return _get_pass()


def get_user() -> str:
    return getpass.getuser()


def get_dir():
    dirpath = osp.join(osp.abspath(osp.expanduser("~")), BIDARKA_DIR)
    if not osp.exists(dirpath):
        os.makedirs(dirpath)
        os.chmod(dirpath, 0o700)
    return dirpath


def _store_pass(password):
    dirpath = get_dir()
    p = base64.b64encode(password.encode('utf-8'))
    outpath = osp.join(dirpath, ENC_FILE)
    logging.debug(outpath)
    with open(outpath, 'wb') as f:
        f.write(p)
        f.flush()


def _get_pass():
    dirpath = get_dir()
    inpath = osp.join(dirpath, ENC_FILE)
    if not osp.exists(inpath):
        prompt_password()
    logging.debug(inpath)
    with open(inpath, 'rb') as f:
        p = f.readline()
        password = base64.b64decode(p.decode('utf-8')).decode('utf-8')
    return password

