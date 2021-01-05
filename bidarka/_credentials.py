import getpass
import logging
import os.path as osp
import os
import tempfile
import subprocess


logging.basicConfig(format='%(message)s', level=logging.INFO)


def prompt_password():
    p = getpass.getpass(prompt="Password:")
    _store_pass(p)


def get_password():
    p = _decrypt_pass()
    return p


def get_user():
    return getpass.getuser()


def _set_dir():
    dirpath = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain")
    if not osp.exists(dirpath):
        os.makedirs(dirpath)
        os.chmod(dirpath, 0o700)
    return dirpath


def ___store_pass(password):
    dirpath = _set_dir()
    outpath = osp.join(dirpath, "encrypt.pass")
    with open(outpath, 'w') as f:
        f.write(password)
        f.flush()


def ___decrypt_pass():
    dirpath = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain")
    outpath = osp.join(dirpath, "encrypt.pass")
    with open(outpath) as f:
        password = f.read()
    return password


def _store_pass(password):
    dirpath = _set_dir()
    script = "openssl enc -aes-128-cbc -in {inbound} -out {outbound} -a -salt -pass pass:{user}"
    with tempfile.NamedTemporaryFile(dir=dirpath) as f:
        f.write(password)
        f.flush()
        outpath = osp.join(dirpath, "encrypt.pass")
        script = script.format(inbound=f.name, outbound=outpath, user=getpass.getuser())
        logging.debug(script)
        rc = subprocess.call(script.split())
        logging.debug(rc)


def _decrypt_pass():
    dirpath = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain")
    script = "openssl enc -aes-128-cbc -in {inbound} -out {outbound} -a -d -salt -pass pass:{user}"
    outpath = osp.join(dirpath, "encrypt.pass")
    if not osp.exists(outpath):
        prompt_password()
    password = None
    with tempfile.NamedTemporaryFile(dir=dirpath) as f:
        outpath = osp.join(dirpath, "encrypt.pass")
        script = script.format(inbound=outpath, outbound=f.name, user=getpass.getuser())
        logging.debug(script)
        rc = subprocess.call(script.split())
        with open(f.name) as pass_file:
            password = pass_file.read()
    return password
