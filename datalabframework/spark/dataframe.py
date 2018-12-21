import pyspark
import pyspark.sql.functions as F

import pandas as pd

from datetime import datetime
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


def filter_by_date(df, date_column=None, date_start=None, date_end=None, date_window=None, date_timezone=None):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame)

    if not date_column:
        return df

    #to datetime and datedelta
    date_window = pd.to_timedelta(date_window) if date_window else None
    date_end = dp.isoparse(date_end) if date_end else None
    date_start = dp.isoparse(date_start) if date_start else None

    # calculate begin and end
    if date_start and date_window and not date_end:
        date_end = date_start + date_window

    if date_end and date_window and not date_start:
        date_start = date_end - date_window

    date_timezone = date_timezone if date_timezone else 'GMT'

    if date_column in df.columns:
        df = df.filter(F.col(date_column) < date_end) if date_end else df
        df = df.filter(F.col(date_column) >= date_start) if date_start else df

    return df

def columns(df_a, df_b=None, exclude_cols=[]):
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
