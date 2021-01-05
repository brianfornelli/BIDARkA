![Underwriting image](docs/BIDARkA_logo.PNG)

<font face="verdana, sans-serif">
<p>
BIDARkA stands for “BIg Data ARchitecture Abstraction tool.”
<br />
The idea is to abstract away the complex parts of the big data
<br />
architecture – sqoop, spark, bteq, hive.
<br />
</p>
</font>

---
<font face="verdana,arial,sans-serif" size="5.0em">
<b>
Developers
</b>
</font>



|Name|E-mail
|:---|---|
|Brian Fornelli|brian.fornelli@gmail.com   |

---

<font face="verdana,arial,sans-serif" size="5.0em">
<b>
Basic API
</b>
</font>

```bash
    beeline(command, payload={})
        Run beeline command
        :param command: beeline command
        :param payload: kv pairs (rare to use this)
        :return:

    bteq(script_path, payload={}) -> None
        Runs a .bteq script
        :param script_path: path/to/bteq
        :param payload: kv pairs to template
        :return:

    csv_hive(db, table, path)
        Convert a CSV table to Hive
        :param db: destination database
        :param table: destination table
        :param path: path/to/csv
        :return:

    hive_csv(db, table, path, orderby=None)
        Convert a Hive table to csv.gz
        :param db: database
        :param table: tablename
        :param path: destination path
        :param orderby: ordering [optinoal]
        :return:

    hive_drop(db, table) -> None
        Safe drop of hive table
        :param db: database
        :param table: tablename
        :return:

    hive_hql(path, payload={}) -> None
        Runs a .hql script via beeline
        :param path: /path/to/hql
        :param payload: kv pair to template
        :return:

    prompt_password()
        Prompt for the password... this should show up in IDE but if it doesn't check the running command-line
        :return:

    read_config(key=None)
        Read the settings file
        :param key:
        :return:

    read_settings(key=None)
        Read settings file
        :param key:
        :return:

    spark(path, payload={}, configuration=None) -> None
        Runs a spark program *.py at given path
        :param path: path/to/sparkjob.py
        :param payload: kv pairs to pass as a configuration
        :param configuration: [optional] spark configuration dictionary items that update spark settings
        :return:

    spark_csr_local(feature_path, meta_path, table_name, n, sort=False, key=None)
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

    sqoop(tdb, ttable, hdb, htable, primary_key=None, num_mappers=8, payload={}) -> None
        Sqoops the data in from Teradata to Hive
        :param tdb: Teradata database
        :param ttable: Teradata table
        :param hdb: Hive database
        :param htable: Hive table
        :param primary_key: key to use for split/partitions [optional]
        :param num_mappers: number of mappers [optional]
        :param payload: key, value pairs to pass to update behavior [optional]
        :return:

    tapeworm(idb, itbl, odb, otbl, primary_key, col_path)
        Converts a dense table into a sparse format in Hive

        :param idb: inbound database
        :param itbl: inbound table
        :param odb:  outbound database
        :param otbl: outbound table
        :param primary_key: primary key for the table
        :param col_path: path to a text file one line for each feature
        :return:
```

---

<font face="verdana,arial,sans-serif" size="5.0em">
<b>
Command-line interface (new)
</b>
</font>

```bash
bidarka sqoop tdb=tdb ttable=ttable hdb=hdb htable=htable primary_key=None num_mappers=8
bidarka csv_hive db=databasename table=tablename path=path_to_input.csv.gz
bidarka hive_csv db=databasename table=tablename path=path_to_output orderby=None
bidarka hive_drop db=databasename table=tablename
bidarka prompt_password
bidarka spark_csr_local feature_path=outpath.npz meta_path=metapath.csv table_name=hive_db.lil_table n=num_features sort=False key=None
```
---

<font face="verdana,arial,sans-serif" size="5.0em">
<b>
Run settings
</b>
</font>
