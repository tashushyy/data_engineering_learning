from pyspark.sql import functions as F
import re


def normalize_strings(df, string_columns):
    """
    This function takes a dataframe (df) and an array of string columns as arguments
    This function iterates through the list of string columns in the dataframe and
    1. Replaces underscores with spaces
    2. Capitalizes the first letter of each word
    3. Trims trailing whitespace
    4. Replaces empty strings with null
    """
    for colm in string_columns:
        df = df.withColumn(colm, F.regexp_replace(F.col(colm), '_', ' '))
        df = df.withColumn(colm, F.initcap(F.col(colm)))
        df = df.withColumn(colm, F.trim(F.col(colm)))
        df = df.withColumn(colm, F.when(F.col(colm) != "", F.col(colm)).otherwise(None))

    return df


def normalize_column_names(df):
    """
    This function take a dataframe (df) as an argument
    This function calls the normalize_column_name function to convert all column names in the dataframe to snake_case
    """
    return df.select(
        *(F.col(f"`{colm}`").alias(normalize_column_name(colm)) for colm in df.columns)
    )


def normalize_column_name(name):
    """
    This function takes a single column name (string) as an argument
    This function:
    1. Trims trailing whitespace
    2. Converts from camelCase to snake_case
    3. Collapes any sequential underscores into a single underscore
    4. Removes any trailing underscores
    """
    name = name.strip()
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    name = re.sub(r'[^a-zA-Z0-9_]', r'_', name)
    name = re.sub(r'_+', r'_', name)
    name = re.sub(r'_$', r'', name)
    return name.lower()
