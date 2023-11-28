# Import PySpark and sqlalchemy libraries
import pyspark
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import datetime

# Create a Spark session to connect to Oracle
spark = SparkSession.builder.appName("PySpark SCD Type 2 Example").getOrCreate()
engine = create_engine('oracle://scott:scott@orcl', echo=False)

# Read data from the source table into a DataFrame
emp_df = spark.read.format('jdbc').options(
    url='jdbc:oracle:thin:@orcl',
    user='scott',
    password='scott',
    dbtable='emp_src'
).load()

# Read data from the target table into a DataFrame
emp_delta_df = spark.read.format('jdbc').options(
    url='jdbc:oracle:thin:@orcl',
    user='scott',
    password='scott',
    dbtable='emp_scd2'
).load()

# Check if the target table is empty
if emp_delta_df.count() == 0:
    # If the target table is empty, perform a full load
    # Create a new DataFrame with the columns and flag for the full load
    e_fload_df = emp_df.select('empno', 'ename', 'sal', 'deptno')
    e_fload_df = e_fload_df.withColumn('flag', pyspark.sql.functions.lit(1)) # Flag 1 indicates the latest record
    # Write the full load DataFrame to the target table
    e_fload_df.write.format('jdbc').options(
        url='jdbc:oracle:thin:@orcl',
        user='scott',
        password='scott',
        dbtable='emp_scd2'
    ).mode('append').save()
else:
    # If the target table is not empty, perform a delta load
    # Join the source and target DataFrames on the natural key
    e_join_df = emp_df.join(emp_delta_df, emp_df.empno == emp_delta_df.empno, how='outer')
    # Filter the joined DataFrame to get the new records from the source
    e_new_df = e_join_df.filter(e_join_df.empno.isNull())
    # Filter the joined DataFrame to get the changed records from the source
    e_change_df = e_join_df.filter((e_join_df.empno.isNotNull()) & ((e_join_df.ename != e_join_df.ename) | (e_join_df.sal != e_join_df.sal) | (e_join_df.deptno != e_join_df.deptno)))
    # Filter the joined DataFrame to get the unchanged records from the source
    e_unchange_df = e_join_df.filter((e_join_df.empno.isNotNull()) & (e_join_df.ename == e_join_df.ename) & (e_join_df.sal == e_join_df.sal) & (e_join_df.deptno == e_join_df.deptno))
    # Create a new DataFrame with the columns and flag for the new records
    e_new_df = e_new_df.select('empno', 'ename', 'sal', 'deptno')
    e_new_df = e_new_df.withColumn('flag', pyspark.sql.functions.lit(1)) # Flag 1 indicates the latest record
    # Write the new records DataFrame to the target table
    e_new_df.write.format('jdbc').options(
        url='jdbc:oracle:thin:@orcl',
        user='scott',
        password='scott',
        dbtable='emp_scd2'
    ).mode('append').save()
    # Create a new DataFrame with the columns and flag for the changed records
    e_change_df = e_change_df.select('empno', 'ename', 'sal', 'deptno')
    e_change_df = e_change_df.withColumn('flag', pyspark.sql.functions.lit(1)) # Flag 1 indicates the latest record
    # Write the changed records DataFrame to the target table
    e_change_df.write.format('jdbc').options(
        url='jdbc:oracle:thin:@orcl',
        user='scott',
        password='scott',
        dbtable='emp_scd2'
    ).mode('append').save()
    # Update the flag and expiry date of the old records in the target table
    e_old_df = e_change_df.select('empno')
    e_old_df = e_old_df.withColumn('flag', pyspark.sql.functions.lit(0)) # Flag 0 indicates the old record
    e_old_df = e_old_df.withColumn('expiry_date', pyspark.sql.functions.current_date()) # Expiry date is the current date
    # Create a temporary view of the old records DataFrame
    e_old_df.createOrReplaceTempView('e_old')
    # Execute a SQL query to update the target table using the temporary view
    spark.sql("""
        UPDATE emp_scd2
        SET flag = e_old.flag,
            expiry_date = e_old.expiry_date
        FROM e_old
        WHERE emp_scd2.empno = e_old.empno
        AND emp_scd2.flag = 1
    """)
