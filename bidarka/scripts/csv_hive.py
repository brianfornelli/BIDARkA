from pyspark import SparkContext, HiveContext
import json
import pandas as pd
import argparse
import logging


def _to_hive(sc, csv_path, hive_database, hive_table):
    sqlContext = HiveContext(sc)
    df = pd.read_csv(csv_path)
    logging.info("-"*30)
    logging.info("Read into dataframe")
    sdf = sqlContext.createDataFrame(df)
    logging.info("-"*30)
    logging.info("Created a dataframe in sql context")
    sqlContext.sql("drop table if exists {}.{}".format(hive_database, hive_table))
    sdf.registerTempTable("edwardio_csv_hive")
    s = r"create table {}.{} as select * from edwardio_csv_hive".format(hive_database, hive_table)
    logging.info("_to_hive:s='{}'".format(s))
    sqlContext.sql(s)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=json.loads, required=True)
    args = parser.parse_args()
    config = args.config
    spark_context = SparkContext()
    _to_hive(spark_context, config['csv_path'], config['hive_database'], config['hive_table'])
