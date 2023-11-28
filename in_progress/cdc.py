import sys,ast,os,time
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSesion
from pyspark.sql.window import Window



# reusable functions


def count_check(spark,tablename,filter = "1=1"):
    try:
        count = spark.sql("select * from {tablename} where {filter}".format(tablename = tablename, filter = filter))
        return count
    except Exception as ex:
        print(f"Failed to get hive table{tablename} count : {ex}"

def duplicate_check(spark, tablename, keys, filter = "1=1"):
    try:
        count = spark.sql(f"select * from (select {keys},count(*) from {tablename} where {filter} group by {keys} having count(*)>1) foo ")
        return count
    except Exception as ex:
        print(f"Failed to get hive table {tablename} count : {ex}")

def IncrementalInsertUpdate(spark, tablename, naturalkeys, bucket, tgtdatabase):
    
    print(f"Starting cdc for table {tablename} with Natural Keys {naturalkeys}")
    nkey = naturalkeys
    table = tablename
    masterbucket = bucket
    database = tgtdatabase
    prepfile = masterbucket+f"{table}" + "/prep/"
    tgtfile = masterbucket+f"{table}" + "/tgt/"
    tempfile = masterbucket+f"{table}" + "/temp/"
    currbackup = masterbucket+f"{table}" + "/currbackup/"
    audit_cols = ['audit_flag' , 'bgn_dt', 'end_dt', 'audit_inserted_ts']
    
    #removing duplicates in prep file
    window_dup = Window.partitionBy(col('Naturalkey')).orderBy(col('Naturalkey'))
    prep = spark.read.parquet(prepfile).drop("audit_inserted_ts") \
        .withColumn("Naturalkey", concat_ws("||",nkey)) \
        .withColumn("rnk",row_number().over(window_dup)) \
        .where(rnk = 1) \
        .drop("rnk")
    #abort if prep table is blank
    if prep.count() == 0:
        print(f"Empty datasource. exiting CDC")
    
    #taking backup of current partition
    try:
        curr = spark.read.parquet(tgtfile).where("end_dt = '9999-12-31'")
        curr.coalesce(1).write.mode("overwrite").parquet(currbackup)
        curr_count = curr.count()
    except:# for initial load current data will be empty; so will be considered as initial load
        print(f"Current data is empty, Initiating initial load")
        curr_count = 0
    
    # when we do initial load
    if curr_count == 0:
        window = Window.orderBy(col('Naturalkey'))
        initaildata = prep.withColumn("audit_flag", lit("I"))\
            .withColumn("bgn_dt", current_date()) \
            .withColumn("end_dt", lit("9999-12-31").cast(DateType()))\
            .withColumn("audit_inserted_ts", current_timestamp())\
            .select(*prep.columns,*audit_cols).drop("Naturalkey")
        initaildata.write.partitionBy('end_dt').mode("overwrite").option("path", tgtfile).saveAsTable(
        f"{database}.{table}
        )
    else:
        curr.createOrReplaceTempView(f"{table}_curr")
        prep.createOrReplaceTempView(f"{table}_prep")
        df_NewUpdatedRecord = spark.sql(f"""select a.*
                                            , case when b.{nkey} is null then 'I'
                                                    when b..{nkey} is not null and a.row_md5<>b.row_md5 then 'U'
                                                    when b..{nkey} is not null and a.row_md5 = b.row_md5 then 'Z'
                                            end as flag
                                            from {table}_prep a left outer join {table}_curr b
                                            on trim(upper(a.{nkey})) = trim(upper(b.{nkey}))""")
    
        # getting records having end date in source
        df_DeletedRecordas = spark.sql(f"""select a.* , a.{nkey} as Naturalkey
                                                 , case when b.{nkey} is null then 'D'
                                                 else 'Z'
                                                 end as flag
                                            from {table}_curr a left outer join {table}_prep b
                                            on trim(upper(b.{nkey})) = trim(upper(a.{nkey}))""").where("Flag = 'D'").drop("audit_flag", "bgn_dt", "end_dt", "audit_inserted_ts")
        
        
        ## Updating history for records with flag as 'U'
        # getting updated records and deleted records to update the history
        
        df_NewUpdatedRecord.where("Flag = 'U'").union(df_DeletedRecordas).createOrReplaceTempView("update_Records")
        
        df_history = spark.sql(f"""
            select a.* , a.{nkey} as Naturalkey from {table}_curr a left semi join update_Records b 
            on trim(upper(b.{nkey})) = trim(upper(a.{nkey})) """).drop("end_dt")\
            .withColumn("end_dt",date_add(current_date(),-1))\
            .select(*prep.columns, *audit_cols).drop("Naturalkey")
            
        ## for updated records move the records from current table to the history partition and update the exp date
        df_NewUpdatedRecord = df_NewUpdatedRecord.where("flag in ('I')")\
                                    .withColumn("audit_flag", lit("I"))\
                                    .withColumn("bgn_dt", current_date()) \
                                    .withColumn("end_dt", lit('9999-12-31').cast(DataType())) \
                                    .withColumn("audit_inserted_ts", current_timestamp())\
                                    .drop("flag")\
                                    .select(*prep.columns, *audit_cols).drop("Naturalkey")
        
        df_updRecords = df_NewUpdatedRecord.where("flag in ('U')") \
                                        .withColumn("audit_flag", lit('I')) \
                                        .withColumn("audit_flag", lit("I"))\
                                        .withColumn("bgn_dt", current_date()) \
                                        .withColumn("end_dt", lit('9999-12-31').cast(DataType())) \
                                        .withColumn("audit_inserted_ts", current_timestamp())\
                                        .drop("flag")\
                                        .select(*prep.columns, *audit_cols).drop("Naturalkey")
                                        
        
        ## rebuild the current -union the new records + update records + records with no change
        df_current = df_noUpdRecords.union(df_updRecords).union(df_updRecords)
        
        ## Rebuild the current by union of DF.
        
        df_current_count = df_current.count()
        if df_current_count > 0:
            ## Update the current data only when the data frame has any records.
            df_current.coalesce(1).write.mode("overwrite").parquet(tempfile)
            ##  write the data to temp before we write to target.
            df_current_final = spark.read.parquet(tempfile)
            if df_current_count == df_current_final.count():
                df_current_final.write.partitionBy('end_dt') \ 
                .mode("overwrite").option("path",tgtfile) \ 
                .saveAsTable(f"{database}.{table}")
        
        print(f"CDC completed for table {table} with Naturalkey {nkey}")
        print("collect MSCK repair")
        spark.sql(f"msck repair table {database}.{table}")
        print("MSCK repair completed")