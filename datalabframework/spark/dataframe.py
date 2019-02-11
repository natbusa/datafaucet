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

def view(df, colnames=None, state_col='_state', updated_col='_updated'):
    """
    Calculate a view from a log of events by performing the following actions:
        - squashing the events for each entry record to the last one
        - remove deleted record from the list
    """

    c = set(df.columns).difference({state_col, updated_col})
    colnames = [x for x in df.columns if x in c] if colnames is None else colnames

    if updated_col not in df.columns:
        return df
    
    if state_col not in df.columns:
        return df
    
    row_groups = df.groupBy(colnames)
    df_view = row_groups.agg(F.sort_array(F.collect_list(F.struct( F.col(updated_col), F.col(state_col))),asc = False).getItem(0).alias('_last')).select(*colnames, '_last.*')
    df_view = df_view.filter("{} = 0".format(state_col))

    return df_view

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
    if df_b.count():
        return df_a.select(colnames).subtract(df_b.select(colnames))
    else:
        return df_a.select(colnames)
    
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

def add_hash_column(obj, cols, hash_colname = '_hash', exclude_cols=[]):
    # add the _updated timestamp
    if isinstance(cols, bool) and cols:
        cols =obj.columns
        
    colnames = (set(obj.columns) & set(cols)) - set(exclude_cols)
    cols = [x for x in cols if x in colnames]
    
    obj = obj.withColumn(hash_colname, F.hash(*cols))
    return obj

def empty(df):
    return df.sql_ctx.createDataFrame([],df.schema)

def summary(df):
        spark = df.sql_ctx

        types = {x.name:x.dataType for x in list(df.schema)}
        res = pd.DataFrame.from_dict(types, orient='index')
        res.columns = ['datatype']
        
        d= df.select([F.approx_count_distinct(c).alias(c) for c in df.columns]).toPandas().T
        d.columns = ['approx_distinct']
        d.index.name = 'index'
        res = res.join(d)

        d= df.select([F.count(c).alias(c) for c in df.columns]).toPandas().T
        d.columns = ['count']
        d.index.name = 'index'
        res = res.join(d)

        sel = []
        for c,v in types.items():
            if isinstance(v, (T.NumericType)):
                sel += [F.count(F.when(F.isnan(c), c)).alias(c)]
            else:
                sel += [F.lit(None).alias(c)]
        d = df.select(sel).toPandas().T
        d.columns = ['mean']
        d.index.name = 'index'
        res = res.join(d)

        d= df.select([F.min(c).alias(c) for c in df.columns]).toPandas().T
        d.columns = ['min']
        d.index.name = 'index'
        res = res.join(d)

        d= df.select([F.max(c).alias(c) for c in df.columns]).toPandas().T
        d.columns = ['max']
        d.index.name = 'index'
        res = res.join(d)

        d= df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns]).toPandas().T
        d.columns = ['null']
        d.index.name = 'index'
        res = res.join(d)

        sel = []
        for c,v in types.items():
            if isinstance(v, (T.NumericType)):
                sel += [F.count(F.when(F.isnan(c), c)).alias(c)]
            else:
                sel += [F.lit(0).alias(c)]
        d = df.select(sel).toPandas().T
        d.columns = ['nan']
        d.index.name = 'index'
        res = res.join(d)

        sel = []
        for c,v in types.items():
            if isinstance(v, (T.StringType)):
                sel += [F.count(F.when(F.col(c).isin(''), c)).alias(c)]
            else:
                sel += [F.lit(0).alias(c)]
        d = df.select(sel).toPandas().T
        d.columns = ['empty']
        d.index.name = 'index'
        res = res.join(d)

        return res


