from pyspark import SparkContext, HiveContext
import argparse
import json
import numpy as np
import pandas as pd
from scipy.sparse import vstack, csr_matrix
import itertools


def save_npz(file, matrix, compressed=True):
    arrays_dict = {}
    if matrix.format in ('csc', 'csr', 'bsr'):
        arrays_dict.update(indices=matrix.indices, indptr=matrix.indptr)
    elif matrix.format == 'dia':
        arrays_dict.update(offsets=matrix.offsets)
    elif matrix.format == 'coo':
        arrays_dict.update(row=matrix.row, col=matrix.col)
    else:
        raise NotImplementedError('Save is not implemented for sparse matrix of format {}.'.format(matrix.format))
    arrays_dict.update(
        format=matrix.format.encode('ascii'),
        shape=matrix.shape,
        data=matrix.data
    )
    if compressed:
        np.savez_compressed(file, **arrays_dict)
    else:
        np.savez(file, **arrays_dict)


def _tuple_to_csr(column_arr, val_arr, shape):
    indptr = np.concatenate([[0], np.cumsum(map(lambda x: len(x), column_arr))])
    col = np.array([i for i in itertools.chain.from_iterable(column_arr)])
    data = np.array([i for i in itertools.chain.from_iterable(val_arr)], dtype=np.float)
    return csr_matrix((data, col, indptr), shape=shape)


def run(sc, feature_path, meta_path,  table_name, n, sort, key=None):
    sqlContext = HiveContext(sc)
    df = sqlContext.table(table_name)
    pd_data = df.toPandas()
    if sort:
        print(".*20")
        print("sorting...")
        pd_data.sort_values(by=key, ascending=True, inplace=True)
        print(".*20")

    # Spark data to sparse featureset
    X = _tuple_to_csr(pd_data.colptr, pd_data.val, shape=(pd_data.shape[0], n))
    X.eliminate_zeros()

    columns = [c for c in pd_data.columns if c not in ('colptr', 'val')]
    print("columns selected: ")
    print(columns)
    save_npz(feature_path, X)
    print("storing featureset to {}".format(feature_path))

    odf = pd_data[columns].copy()
    odf.to_csv(meta_path, index=False)
    print("storing metadata to {}".format(meta_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=json.loads, required=True)
    args = parser.parse_args()
    config = args.config
    spark_context = SparkContext()
    key = config["key"]
    sort = config["sort"]
    run(spark_context, config['feature_path'], config['meta_path'], config["table_name"],
        int(config["n"]), sort, key)
