from pyspark import SparkContext, HiveContext
import json
import pandas as pd
import argparse


def _to_csv(sc, sql, local_path):
    sqlContext = HiveContext(sc)
    df = sqlContext.sql(sql).toPandas()
    df.to_csv(local_path, index=None, compression='gzip')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=json.loads, required=True)
    args = parser.parse_args()
    config = args.config
    spark_context = SparkContext()
    _to_csv(spark_context, config['sql'], config['local'])
