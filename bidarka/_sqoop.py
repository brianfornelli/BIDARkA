from _util import read_config, script_path
import subprocess
import logging
from string import Template
import os.path as osp
import os
import tempfile

logging.basicConfig(format='%(message)s', level=logging.INFO)


def sqoop(tdb, ttable, hdb, htable, payload={}):
    config = read_config()
    sqoop_config = config.SQOOP_CONFIG
    outdir = _get_dir()
    script = _read_template()
    try:
        password_file = write_temp_pfile(config.PASSWORD)
        sqoop_config.update(
            dict(USERNAME=config.USERNAME, teradata_database=tdb, teradata_table_inbound=ttable, hive_database=hdb,
                 hive_table_inbound=htable, outdir=outdir, password_file=password_file))
        src = Template(script)
        script_text = src.substitute(sqoop_config, **payload)
        logging.info(script_text)
        subprocess.check_call(script_text.split())
    except Exception, e:
        raise e
    finally:
        os.unlink(password_file)


def _get_dir():
    dirpath = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain/sqoop")
    if not osp.exists(dirpath):
        os.makedirs(dirpath)
    return dirpath


def write_temp_pfile(password):
    home = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain/sqoop")
    f = tempfile.NamedTemporaryFile(delete=False, dir=home)
    f.write(password)
    f.close()
    return f.name


def _read_template():
    path = script_path("templates/sqoop.template")
    with open(path, 'r') as f:
        src = f.read()
    return src


if __name__ == '__main__':
    tdb = 'dl_aa_tm_ds_s'
    ttable = 'uw_grp_val_dense'
    hdb = 'hcaadvaph_wk_playground'
    htable = 'bidarka2_test'
    sqoop(tdb=tdb, ttable=ttable, hdb=hdb, htable=htable)
