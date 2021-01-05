from ._sqoop import sqoop as __sqoop
from ._spark import spark as __spark
from ._bteq import bteq as __bteq
from ._beeline import beeline as __beeline, hive_hql as __hive_hql
from ._credentials import prompt_password as __prompt_password
from ._util import read_settings as __read_settings, script_path


def sqoop(tdb, ttable, hdb, htable, primary_key=None, num_mappers=8, payload={}) -> None:
    """
    Sqoops the data in from Teradata to Hive
    :param tdb: Teradata database
    :param ttable: Teradata table
    :param hdb: Hive database
    :param htable: Hive table
    :param primary_key: key to use for split/partitions [optional]
    :param num_mappers: number of mappers [optional]
    :param payload: key, value pairs to pass to update behavior [optional]
    :return:
    """
    __sqoop(tdb=tdb, ttable=ttable, hdb=hdb, htable=htable, primary_key=primary_key,
            num_mappers=num_mappers, payload=payload)


def spark(path, payload={}, configuration=None) -> None:
    """
    Runs a spark program *.py at given path
    :param path: path/to/sparkjob.py
    :param payload: kv pairs to pass as a configuration
    :param configuration: [optional] spark configuration dictionary items that update spark settings
    :return:
    """
    __spark(path, payload, configuration)


def bteq(script_path, payload={}) -> None:
    """
    Runs a .bteq script
    :param script_path: path/to/bteq
    :param payload: kv pairs to template
    :return:
    """
    __bteq(script_path, payload)


def hive_hql(path, payload={}) -> None:
    """
    Runs a .hql script via beeline
    :param path: /path/to/hql
    :param payload: kv pair to template
    :return:
    """
    __hive_hql(path, payload)


def hive_drop(db, table) -> None:
    """
    Safe drop of hive table
    :param db: database
    :param table: tablename
    :return:
    """
    __beeline("drop table if exists {db}.{table};".format(db=db, table=table))


def beeline(command, payload={}):
    """
    Run beeline command
    :param command: beeline command
    :param payload: kv pairs (rare to use this)
    :return:
    """
    __beeline(command, payload)


def prompt_password():
    """
    Prompt for the password... this should show up in IDE but if it doesn't check the running command-line
    :return:
    """
    __prompt_password()


def read_config(key=None):
    """
    Read the settings file
    :param key:
    :return:
    """
    config = __read_settings()
    if key is None:
        return config
    else:
        return getattr(config, key, None)


def read_settings(key=None):
    """
    Read settings file
    :param key:
    :return:
    """
    return read_config(key)


def spark_csr_local(feature_path, meta_path, table_name, n, sort=False, key=None):
    """
    Construct a localized version of the training set and bring it down to `feature_path` and metadata to `meta_path`
    so that you can do modeling work.

    Note:  This assumes that there is a primary key `mcid`.
    :param feature_path:  *npz file where features will be stored as a CSR matrix format
    :param meta_path:  *.csv file where the metadata will be stored
    :param table_name: The target table to read from in Hive (Note:  this needs to be in our LiL format)
    :param n: The number of features/variables (assume a mXn matrix)
    :param sort:  (Default False)
    :param key:  Key for sorting ignored if sorted=False
    :return: None
    """
    path = script_path("scripts/build_csr_matrix.py")
    spark(path, payload=dict(feature_path=feature_path, meta_path=meta_path,
                             table_name=table_name, n=n, sort=sort, key=key))


def tapeworm(idb, itbl, odb, otbl, primary_key, col_path):
    """
    Converts a dense table into a sparse format in Hive

    :param idb: inbound database
    :param itbl: inbound table
    :param odb:  outbound database
    :param otbl: outbound table
    :param primary_key: primary key for the table
    :param col_path: path to a text file one line for each feature
    :return:
    """
    with open(col_path, "r") as f:
        lines = f.readlines()
    column_list = ",".join(lines)
    worm_path = script_path("scripts/tapeworm.hql")
    hive_hql(path=worm_path, payload=dict(idb=idb, itbl=itbl, odb=odb, otbl=otbl, primary_key=primary_key,
                                          column_list=column_list))


def hive_csv(db, table, path, orderby=None):
    """
    Convert a Hive table to csv.gz
    :param db: database
    :param table: tablename
    :param path: destination path
    :param orderby: ordering [optinoal]
    :return:
    """
    if orderby is None:
        sql = "select * from {}.{}".format(db, table)
    else:
        sql = "select * from {}.{} order by {}".format(db, table, orderby)
    payload = dict(local=path, sql=sql)
    filepath = script_path('scripts/hive_csv.py')
    spark(filepath, payload=payload)


def csv_hive(db, table, path):
    """
    Convert a CSV table to Hive
    :param db: destination database
    :param table: destination table
    :param path: path/to/csv
    :return:
    """
    payload = dict(csv_path=path, hive_database=db, hive_table=table)
    filepath = script_path('scripts/csv_hive.py')
    spark(filepath, payload=payload)
