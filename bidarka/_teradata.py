from _util import read_config, script_path, Command
import subprocess
import logging
from string import Template
import os.path as osp
import os
import shutil
import tempfile
import atexit
from collections import defaultdict
import pandas as pd
import teradata
from __version__ import __version__

logging.basicConfig(format='%(message)s', level=logging.INFO)


def fetch_fast_df(db, tbl, index_column='indx'):
    params = _fetch_meta(db, tbl, index_column)
    config = read_config()
    td_config = Command(config.TERADATA_CONFIG)
    td_config.update(params)
    tpt_path = script_path("templates/tpt.template")
    with _TPTFetch() as (data_path, tpt_script_path):
        td_config.update(dict(username=config.USERNAME, password=config.PASSWORD, tpt_temp_data=data_path))
        qry = _apply_file_template(tpt_path, kv=td_config)
        _prepare_tpt(tpt_script_path, qry)
        subprocess.call(["tbuild", "-f", tpt_script_path])
        df = pd.read_csv(data_path, sep=',', index_col=None, header=None, names=params['extract_columns'])
    return df


def fetch_odbc_df(sql, configuration={}):
    with _odbc_connection(configuration=configuration) as session:
        cursor = session.execute(sql)
        df = _fetch_odbc_df(cursor)
    return df


def execute_script(script, configuration={}):
    with _odbc_connection(configuration=configuration) as session:
        session.execute(file=script)


def _fetch_odbc_df(cursor):
    output = defaultdict(list)
    for row_result in cursor:
        for v in row_result.columns:
            output[v].append(row_result[v])
    key_order = map(lambda i: i[0], sorted(row_result.columns.items(), key=lambda (k, v): v))
    for i, v in enumerate(key_order):
        if i == 0:
            df = pd.DataFrame(output[v], columns=[v])
        else:
            df[v] = output[v]
    return df


def _odbc_connection(configuration):
    config = read_config()
    td_config = Command(config.TERADATA_CONFIG)
    td_config.update(configuration)
    if not os.environ.has_key('ODBCINI'):
        os.environ['ODBCINI'] = td_config.ODBCINI
    uda_exec = teradata.UdaExec(appName=td_config.appName, version=__version__, logConsole=False)
    return uda_exec.connect(method=td_config.method, system=td_config.system, authentication=td_config.authentication,
                            username=config.USERNAME, password=config.PASSWORD)


class _TPTFetch(object):
    def __init__(self):
        self._temp_folder = tempfile.mkdtemp()
        dirpath = osp.join(osp.abspath(osp.expanduser("~")), "ds_keychain")
        self._script_path = tempfile.NamedTemporaryFile(dir=dirpath, prefix="tpt_", delete=False)
        self._data_path = osp.join(self._temp_folder, 'output_data.txt')

    def __enter__(self):
        atexit.register(lambda : _delete_folder(self._temp_folder))
        atexit.register(lambda : _delete_file(self._script_path.name))
        return self._data_path, self._script_path.name

    def __exit__(self, exc_type, exc_val, exc_tb):
        _delete_folder(self._temp_folder)
        _delete_file(self._script_path.name)


def _delete_folder(folder_path):
    if osp.exists(folder_path):
        shutil.rmtree(folder_path)


def _delete_file(path):
    if osp.exists(path):
        os.remove(path)


def _apply_file_template(template, kv):
    with open(template, 'r') as f:
        src = Template(f.read())
        result = src.substitute(kv)
    return result


def _fetch_meta(db, tbl, indx=None):
    meta_path = script_path("templates/metadata.template")
    qry = _apply_file_template(meta_path, kv=dict(database=db, tablename=tbl))
    df = fetch_odbc_df(qry)

    extract_columns = [e for e in df['columnname'].str.strip()]
    txt = [e for e in df['txt'].str.strip()]
    extract_schema = _extract_schema(extract_columns, txt)
    if indx is None:
        extract_sql = "select * from {0}.{1}".format(db, tbl)
    else:
        extract_sql = "select * from {0}.{1} order by {2}".format(db, tbl, indx)
    return dict(extract_columns=extract_columns, extract_schema=extract_schema, extract_sql=extract_sql)


def _extract_schema(extract_columns, datatypes):
    x = ["{0} {1}".format(*i) for i in zip(extract_columns, datatypes)]
    extract_schema = ','.join(x)
    return extract_schema


def _prepare_tpt(tmp, qry):
    with open(tmp, "w") as f:
        f.writelines(qry)
        f.flush()


if __name__ == '__main__':
    #df = fetch_odbc_df("select current_date")
    #logging.info(df)
    #metadata = _fetch_meta("dl_aa_tm_cons_s", "ugly_skinny")
    #logging.info(metadata)
    df = fetch_fast_df("dl_aa_tm_cons_s", "ugly_skinny", "indx")
    print(df)

