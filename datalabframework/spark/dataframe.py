import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

from datetime import datetime
import pytz

import pandas as pd
import dateutil.parser as dp

def repartition(df, repartition=None):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame)
    return df.repartition(repartition) if repartition else df

def coalesce(df, coalesce=None):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame)
    return df.coalesce(coalesce) if coalesce else df

def cache(df, cached=False):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame)
    return df.cache() if cached else df


def filter_by_date(df, column=None, start=None, end=None, window=None):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame)
    
    if not column:
        return df
    
    #to datetime and datedelta
    date_window = pd.to_timedelta(window) if window else None
    date_end = dp.isoparse(end) if end else None
    date_start = dp.isoparse(start) if start else None

    # calculate begin and end
    if date_start and date_window and not date_end:
        date_end = date_start + date_window

    if date_end and date_window and not date_start:
        date_start = date_end - date_window

    if column in df.columns:
        df = df.filter(F.col(column) < date_end) if date_end else df
        df = df.filter(F.col(column) >= date_start) if date_start else df
    
    # print(column, date_start, date_end, df.count())

    return df

def common_columns(df_a, df_b=None, exclude_cols=[]):
    """
    Common columns given two dataframes, preserve the order of the  second dataframe

    :param df_a: first dataframe
    :param df_b:  second dataframe
    :param exclude_cols: column names to exclude
    :return: a list of column names, preserving the order
    """

    assert isinstance(df_a, pyspark.sql.dataframe.DataFrame)

    colnames_a = set(df_a.columns if df_a else [])
    colnames_b = set(df_b.columns if df_b else [])
    colnames = colnames_a & colnames_b

    c = colnames - set(exclude_cols)

    # prefer df_b column order
    cols =  df_b.columns if df_b else df_a.columns

    # return a common column list in the same order
    # as provided by df_b or df_a column method
    return [x for x in cols if x in c]

def diff(df_a, df_b, exclude_cols=[]):
    """
    Returns all columns of a which are not in b.
    Column ordering as provided by the second dataframe

    :param df_a: first dataframe
    :param df_b: second dataframe
    :param exclude_cols: columns to be excluded
    :return: a diff dataframe
    """

    assert isinstance(df_a, pyspark.sql.dataframe.DataFrame)
    assert isinstance(df_b, pyspark.sql.dataframe.DataFrame)

    # get columns
    colnames = common_columns(df_a, df_b, exclude_cols)

    # return diff
    return df_a.select(colnames).subtract(df_b.select(colnames))

def to_timestamp(obj, column, tzone='UTC'):
    f = F.col(column)
    datecol_type = obj.select(column).dtypes[0][1]

    if datecol_type not in ['timestamp', 'date']:
        f = F.to_timestamp(f)

    if tzone != 'UTC':
        f = F.to_utc_timestamp(f, tzone)

    return f

def add_datetime_columns(obj, column=None, date_col = '_date', datetime_col = '_datetime', tzone='UTC'):
    if column in obj.columns:
        obj = obj.withColumn(datetime_col, to_timestamp(obj, column, tzone))
        obj = obj.withColumn(date_col, F.to_date(datetime_col))
    return obj

def add_update_column(obj, updated_colname = '_updated', tzone='UTC'):
    # add the _updated timestamp
    ts = datetime.strftime(datetime.now(pytz.timezone(tzone if tzone else 'UTC')), '%Y-%m-%d %H:%M:%S')
    obj = obj.withColumn(updated_colname, F.lit(ts).cast(T.TimestampType()))
    return obj

def add_hash_column(obj, cols, hash_colname = '_hash'):
    # add the _updated timestamp
    if isinstance(cols, bool) and cols:
        cols = obj.columns
        
    obj = obj.withColumn(hash_colname, F.hash(*cols))
    return obj

def empty(df):
    return df.sql_ctx.createDataFrame([],df.schema)
