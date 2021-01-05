import unittest
from bidarka import beeline, bteq, csv_hive, hive_csv, hive_drop, hive_hql, prompt_password, read_config, \
    read_settings, spark, spark_csr_local, sqoop, tapeworm

import os.path as osp


def script_path(key):
    path = osp.join(osp.dirname(__file__), key)
    return path


class TestBidarka(unittest.TestCase):
    def test_beeline(self):
        try:
            beeline("show databases")
        except:
            self.fail("beeline() raised exception")

    def test_bteq(self):
        try:
            path = script_path('resources/test_bteq.bteq')
            bteq(script_path=path)
        except:
            self.fail("bteq() raised exception")

    def test_bteq_rc(self):
        import subprocess
        path = script_path('resources/test_bteq_rc.bteq')
        self.assertRaises(subprocess.CalledProcessError, bteq, path)

    def test_sqoop(self):
        try:
            sqoop(tdb='edw_nophi', ttable='clm_sor_cd', hdb='ds_dtlpiadph_gbd_r000_wk', htable='test_clm_sor_cd')
        except:
            self.fail("sqoop() raised an exception")

    def test_hive_drop(self):
        try:
            hive_drop(db="ds_dtlpiadph_gbd_r000_wk", table="test_clm_sor_cd")
        except:
            self.fail("hive_drop() raised an exception")

    def test_hive_hql(self):
        try:
            path = script_path("resources/test_hive.hql")
            hive_hql(path, payload=dict(databases="databases"))
        except:
            self.fail("hive_hql() raised an exception")


if __name__ == '__main__':
    unittest.main()
