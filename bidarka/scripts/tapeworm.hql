set hive.execution.engine=spark;
set spark.executor.instances=10;
set spark.executor.cores=4;
set hive.exec.parallel = true;
set mapreduce.job.name="BIDARkA_tapeworm_ad09492";

/***
 ***  Tapeworms make fat things skinny.  This tapeworm is no different.  Lets take a very fat table that is totally
 ***  useless and put it in a format that is useful for modeling.
 ***
 ***  Parameters:
 ***  idb - the input database
 ***  itbl - the input (fat) table
 ***  odb - the output database
 ***  otbl - the output (skinny) table
 ***  column_list - a big string of comma seperated columns
 ***  primary_key - the primary key for the table
 ***/
drop table if exists ${odb}.${otbl};
create table ${odb}.${otbl}
as
select ${primary_key},
    colptr,
    val
from
(
    select tmp.${primary_key}, collect_list(tmp.colptr) as colptr, collect_list(tmp.val) as val
    from
    (
        select x.${primary_key}, x.colptr, x.val
        from
        (
            select ${primary_key}, colptr, val
            from ${idb}.${itbl} z
            lateral view posexplode(array(
                ${column_list}
            )) cv as colptr, val
            where val <> 0
        ) x
        cluster by x.${primary_key}
    ) tmp
    group by tmp.${primary_key}
) l
;
