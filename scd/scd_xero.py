# Import PySpark and sqlalchemy libraries
import pyspark
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Create a Spark session to connect to Oracle
spark = SparkSession.builder.appName("PySpark Delta Lake - SCD0 Example").getOrCreate()
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
    dbtable='emp_scd0'
).load()

# Check if the target table is empty
if emp_delta_df.count() == 0:
    # If the target table is empty, perform a full load
    # Write the source DataFrame to the target table
    emp_df.write.format('jdbc').options(
        url='jdbc:oracle:thin:@orcl',
        user='scott',
        password='scott',
        dbtable='emp_scd0'
    ).mode('append').save()
else:
    # If the target table is not empty, perform a delta load
    # Join the source and target DataFrames on the natural key
    e_join_df = emp_df.join(emp_delta_df, emp_df.empno == emp_delta_df.empno, how='outer')
    # Filter the joined DataFrame to get the new records from the source
    e_new_df = e_join_df.filter(e_join_df.empno.isNull())
    # Write the new records DataFrame to the target table
    e_new_df.write.format('jdbc').options(
        url='jdbc:oracle:thin:@orcl',
        user='scott',
        password='scott',
        dbtable='emp_scd0'
    ).mode('append').save()
