from pyspark import SparkContext, HiveContext
import argparse
import json


def run(sc, test):
    sqlContext = HiveContext(sc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=json.loads, required=True)
    args = parser.parse_args()
    config = args.config
    spark_context = SparkContext()
    run(spark_context, config['test'])
