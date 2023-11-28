from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def cast_to_string(df, string_columns):
    """
    This function takes a dataframe (df) and an array of columns as arguments
    This function iterates through the list of columns in the dataframe and
    converts them to string types
    """
    for colm in string_columns:
        df = df.withColumn(colm, F.col(colm).cast(StringType()))
    return df


def cast_to_date(df, string_columns, date_format):
    """
    This function takes a dataframe (df), an array of string columns, and a date format (string) as arguments
    This function iterates through the list of string columns in the dataframe and
    converts them to date types based on the specified date format
    Example date format: "MM-dd-yyyy"
    """
    for colm in string_columns:
        df = df.withColumn(colm, F.to_date(F.col(colm), date_format))
    return df
