#!/usr/bin/env python
import argparse
import logging

from .bidarka import csv_hive, hive_csv, hive_drop, prompt_password, spark_csr_local, sqoop, tapeworm

logging.basicConfig(level=logging.INFO)

JOB = dict(csv_hive=csv_hive, hive_csv=hive_csv, hive_drop=hive_drop, prompt_password=prompt_password,
           spark_csr_local=spark_csr_local, sqoop=sqoop, tapeworm=tapeworm)


def _parameterize(val):
    d = {}
    for i in val:
        x = i.replace(" ", "")
        s = x.split("=")
        d.update({s[0]: s[1]})
    return d


def _inspect_concept():
    import inspect
    args = inspect.getfullargspec(csv_hive).args


def main():
    parser = argparse.ArgumentParser(description="Command-line tool for Specialty RX")
    parser.add_argument('job', choices=JOB.keys())
    args, params = parser.parse_known_args()
    dparams = _parameterize(params)
    JOB[args.job](**dparams)


if __name__ == '__main__':
    main()
