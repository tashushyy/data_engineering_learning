# Import PySpark and sqlalchemy libraries
import pyspark
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Create a Spark session to connect to Oracle
spark = SparkSession.builder.appName("PySpark SCD Type 1 Example").getOrCreate()
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
    dbtable='emp_scd1'
).load()

# Join the source and target DataFrames on the natural key
e_join_df = emp_df.join(emp_delta_df, emp_df.empno == emp_delta_df.empno, how='outer')

# Filter the joined DataFrame to get the new records from the source
e_new_df = e_join_df.filter(e_join_df.empno.isNull())

# Filter the joined DataFrame to get the changed records from the source
e_change_df = e_join_df.filter((e_join_df.empno.isNotNull()) & ((e_join_df.ename != e_join_df.ename) | (e_join_df.sal != e_join_df.sal) | (e_join_df.deptno != e_join_df.deptno)))

# Write the new records DataFrame to the target table
e_new_df.write.format('jdbc').options(
    url='jdbc:oracle:thin:@orcl',
    user='scott',
    password='scott',
    dbtable='emp_scd1'
).mode('append').save()

# Write the changed records DataFrame to the target table using the overwrite mode
e_change_df.write.format('jdbc').options(
    url='jdbc:oracle:thin:@orcl',
    user='scott',
    password='scott',
    dbtable='emp_scd1'
).mode('overwrite').save()
