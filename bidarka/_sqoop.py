from ._util import read_settings, read_template, template_transform
from ._credentials import get_dir, get_user, get_password
import subprocess
import logging
import tempfile
import os
import os.path as osp

logging.basicConfig(format='%(message)s', level=logging.INFO)


def sqoop(tdb, ttable, hdb, htable, primary_key=None, num_mappers=8, payload={}) -> None:
    settings = read_settings()
    settings.USERNAME = get_user()
    settings.PASSWORD = get_password()
    sqoop_config = settings.SQOOP_CONFIG
    outdir = _get_dir()
    script = read_template("templates/sqoop.template")
    if primary_key is not None:
        d = dict(num_mappers_statement="--create-hive-table -m {}".format(num_mappers),
                 primary_key_statement="--split-by {}".format(primary_key))
    else:
        d = dict(num_mappers_statement="--create-hive-table -m 1", primary_key_statement="")
    payload.update(d)
    try:
        password_file = _write_temp_pfile(directory=outdir, password=settings.PASSWORD)
        sqoop_config.update(dict(USERNAME=settings.USERNAME, teradata_database=tdb, teradata_table_inbound=ttable,
                                 hive_database=hdb, hive_table_inbound=htable, outdir=outdir,
                                 password_file=password_file))
        script_text = template_transform(script, sqoop_config, payload)
        subprocess.check_call(script_text.split())
    except Exception as e:
        raise e
    finally:
        os.unlink(password_file)


def _get_dir():
    directory = get_dir()
    home = osp.join(directory, 'sqoop')
    if not osp.exists(home):
        os.makedirs(home)
    return home


def _write_temp_pfile(directory, password):
    f = tempfile.NamedTemporaryFile(delete=False, dir=directory, mode="w")
    logging.debug("Writing password file here: {}".format(f.name))
    f.write(password)
    f.close()
    return f.name
