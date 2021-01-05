from _util import read_config
from impala.dbapi import connect
from impala.util import as_pandas


class HiveConnect(object):
    def __init__(self):
        config = read_config()
        impala_config = config.IMPALA_CONFIG
        self.host = impala_config["host"]
        self.port = impala_config["port"]
        self.auth_mechanism = impala_config["auth_mechanism"]
        self.use_ssl = impala_config["use_ssl"]
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = connect(host=self.host, port=self.port, auth_mechanism=self.auth_mechanism, use_ssl=self.use_ssl)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()


def fetch_hive_df(sql):
    df = None
    with HiveConnect() as conn:
        conn.execute(sql)
        df = as_pandas(conn)
    return df


def invalidate_metadata(db, tbl):
    with HiveConnect() as conn:
        conn.execute("invalidate metadata {}.{}".format(db, tbl))
