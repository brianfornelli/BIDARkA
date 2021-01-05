from pyspark.sql import SparkSession
import os.path as osp
import argparse
import json


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=json.loads, required=True)
    args = parser.parse_args()
    config = args.config
    warehouse_location = osp.abspath('spark-warehouse')
    hc = SparkSession.\
        builder.\
        appName("BIDARkA_test_spark").\
        config('spark.sql.warehouse.dir', warehouse_location).\
        enableHiveSupport().\
        getOrCreate()

    hc.sql("show databases")
