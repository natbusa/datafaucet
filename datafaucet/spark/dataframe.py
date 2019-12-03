import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window

import re

import datetime as dt
import pytz

import pandas as pd
import dateutil.parser as dp


def columns(df, *by_regex, by_type=None, by_func=None):
    by_type = by_type if isinstance(by_type, (list, tuple)) else [by_type] if by_type else []
    cols = {x.name: x.dataType for x in list(df.schema)}

    if len(by_regex):
        c = {}
        for r in by_regex:
            regex = re.compile(r)
            c.update({k: v for k, v in cols.items() if regex.search(k)})
        cols = c

    # filter datatypes
    if by_type:
        d = {}
        for k, v in cols.items():
            if str(v) in by_type:
                d.update({k: v})

            if any([x in [t.typeName() for t in type(v).mro() if issubclass(t, type(T.DataType()))] for x in by_type]):
                d.update({k: v})

            if any([isinstance(v, type(x)) for x in by_type if isinstance(x, T.DataType)]):
                d.update({k: v})

        cols = d

    if by_func is not None:
        cols = {k: v for k, v in cols.items() if by_func(k)}

    return list(cols.keys())


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
    cols = df_b.columns if df_b else df_a.columns

    # return a common column list in the same order
    # as provided by df_b or df_a column method
    return [x for x in cols if x in c]


def view(df, state_col='_state', updated_col='_updated', merge_on=None, version=None):
    """
    Calculate a view from a log of events by performing the following actions:
        - squashing the events for each entry record to the last one
        - remove deleted record from the list
    """

    c = set(df.columns).difference({state_col, updated_col})
    colnames = [x for x in df.columns if x in c]

    if updated_col not in df.columns:
        df = add_update_column(df, '_updated')

    if state_col not in df.columns:
        df = df.withColumn('_state', F.lit(0))

    on = merge_on or colnames
    on = on if isinstance(on, (list, tuple)) else [on]

    groupby_columns = [c for c in on if c in colnames]

    #filter version
    df = filter_by_version(df, '_updated', version)

    # group by
    row_groups = df.select(*groupby_columns, '_updated').groupBy(groupby_columns)

    # sort each group by descending _updated
    get_sorted_array = F.sort_array(F.collect_list(F.col('_updated')), asc=False)

    # get the selected rows (only group columns and _updated)
    df_view = row_groups.agg(get_sorted_array.getItem(0).alias('_updated'))

    # get all original columns by join
    df_res = df.join(df_view, on=[*groupby_columns, '_updated'])

    # remove deleted records and _updated/_state columns
    return df_res.filter('_state =0').select(*colnames)


def scd_analyze(df, merge_on=None, state_col='_state', updated_col='_updated'):
    add_ids = '##add_ids'
    del_ids = '##del_ids'
    upd_ids = '##upd_ids'

    c = set(df.columns).difference({state_col, updated_col})
    colnames = [x for x in df.columns if x in c]

    on = merge_on or colnames
    on = on if isinstance(on, (list, tuple)) else [on]
    on = [c for c in on if c in colnames]

    s = on + [state_col, updated_col]
    cols = [x for x in df.columns if x not in s]

    a = df.filter(f'{state_col} = 0') \
        .groupby(updated_col) \
        .agg(F.collect_set(F.concat(*on)).alias(add_ids)) \
        .select(updated_col, add_ids)

    d = df.filter(f'{state_col} = 1') \
        .groupby(updated_col) \
        .agg(F.collect_set(F.concat(*on)).alias(del_ids)) \
        .select(updated_col, del_ids)

    res = a.join(d, on=updated_col, how='outer')
    res = res.select(
        updated_col,
        F.coalesce(add_ids, F.array([])).alias(add_ids),
        F.coalesce(del_ids, F.array([])).alias(del_ids)
    )

    if cols:
        agg_funcs = [(F.countDistinct(x) - F.lit(1)).alias(x) for x in cols]
        cnt = df.groupby(*on, updated_col).agg(*agg_funcs)

        agg_names = [F.lit(x) for x in cols]
        agg_sums = [F.sum(x) for x in cols]
        cnt = cnt.groupby(updated_col).agg(F.map_from_arrays(F.array(*agg_names), F.array(*agg_sums)).alias('changes'))

        res = res.join(cnt, on=updated_col)
    else:
        res = res.withColumn('changes', F.lit(None))

    res = res.select('*', F.array_intersect(add_ids, del_ids).alias(upd_ids))
    res = res.select(
        F.col(updated_col).alias('updated'),
        F.size(upd_ids).alias('upd'),
        F.size(F.array_except(add_ids, upd_ids)).alias('add'),
        F.size(F.array_except(del_ids, upd_ids)).alias('del'),
        'changes'
    )

    return res.orderBy('updated')

def diff(df_a, df_b, exclude_cols=[]):
    """
    Returns all rows of a which are not in b.
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

def filter_by_version(df, column, version):
    # filter version
    if type(version) == int:
        pdf = df.select('_updated').groupby('_updated').count().toPandas()
        pdf = pdf.sort_values('_updated').reset_index(drop=True)
        total_versions = pdf.shape[0]
        version = version % total_versions
        try:
            version = pdf.iloc[version, 0]
        except IndexError:
            raise IndexError('Version index out of bounds: min=0, max = ...')

    if isinstance(version, str):
        version = dp.isoparse(version) if version else None

    return filter_by_datetime(df, column, end=version, closed='right')

def filter_by_datetime(df, column, start=None, end=None, window=None, closed='left'):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame)

    # cast str to datetime and datedelta
    if isinstance(window, str):
        window = pd.to_timedelta(window) if window else None

    if isinstance(end, str):
        end = dp.isoparse(end) if end else None

    if isinstance(start, str):
        start = dp.isoparse(start) if start else None

    # calculate begin and end
    if start and window and not end:
        end = start + window

    if end and window and not start:
        start = end - window

    if column in df.columns:
        cut_right  = (F.col(column) < end) if closed=='left' else (F.col(column) <= end)
        cut_left = (F.col(column) > start) if closed=='right' else (F.col(column) >= start)

        df = df.filter(cut_right) if end else df
        df = df.filter(cut_left) if start else df

    return df


def to_timestamp(obj, column, tzone='UTC'):
    f = F.col(column)
    datecol_type = obj.select(column).dtypes[0][1]

    if datecol_type not in ['timestamp', 'date']:
        f = F.to_timestamp(f)

    if tzone != 'UTC':
        f = F.to_utc_timestamp(f, tzone)

    return f


def add_datetime_columns(obj, column=None, date_col='_date', datetime_col='_datetime', tzone='UTC'):
    if column in obj.columns:
        obj = obj.withColumn(datetime_col, to_timestamp(obj, column, tzone))
        obj = obj.withColumn(date_col, F.to_date(datetime_col))
    return obj


def add_update_column(obj, updated_colname='_updated', tzone='UTC'):
    # add the _updated timestamp
    ts = dt.datetime.strftime(dt.datetime.now(pytz.timezone(tzone if tzone else 'UTC')), '%Y-%m-%d %H:%M:%S')
    obj = obj.withColumn(updated_colname, F.lit(ts).cast(T.TimestampType()))
    return obj


def add_hash_column(obj, cols=True, hash_colname='_hash', exclude_cols=[]):
    # add the _updated timestamp
    if isinstance(cols, bool) and cols:
        cols = obj.columns

    colnames = (set(obj.columns) & set(cols)) - set(exclude_cols)
    cols = [x for x in cols if x in colnames]

    obj = obj.withColumn(hash_colname, F.hash(*cols))
    return obj


def empty(df):
    return df.sql_ctx.createDataFrame([], df.schema)

def sample(df, n=1000, *col, seed=None):
    # n 0<float<=1 -> fraction of samples
    # n floor(int)>1 -> number of samples

    # todo:
    # n dict of key, value pairs or array of (key, value)
    # cols = takes alist of columns for sampling if more than one column is provided
    # if a stratum is not specified, provide equally with what is left over form the total of the other quota

    if n > 1:
        count = df.count()
        fraction = n / count
        return df if fraction > 1 else df.sample(False, fraction, seed=seed)
    else:
        return df.sample(False, n, seed=seed)

def _topn(df, c, by=None, n=3):
    cnt = f'{c}##cnt'
    rnk = f'{c}##rnk'

    _gcols = [by] if isinstance(by, str) and by else by or []
    s = df.select(*_gcols, c).groupby(*_gcols, c).agg(F.count(F.col(c)).alias(cnt))

    # calculate topn
    a = s.select(
        *_gcols, c, cnt, F.row_number().over(
            Window.partitionBy(*_gcols).orderBy(F.col(cnt).desc())
        ).alias(rnk)).filter(F.col(rnk) <= n)

    return s, a

def topn_count(df, c, by=None, n=3, index='_idx', result='_res'):
    cnt = f'{c}##cnt'
    _gcols = [by] if isinstance(by, str) and by else by or []

    s , a = _topn(df, c, _gcols, n)
    r = a.select(*_gcols, c, cnt).groupby(*_gcols).agg(F.sum(cnt).alias(result))

    # the following forces colname to be nullable
    r = r.withColumn(index, F.udf(lambda x: x, T.StringType())(F.lit(c)))

    return r


def topn_values(df, c, by=None, n=3, index='_idx', result='_res'):
    cnt = f'{c}##cnt'
    _gcols = [by] if isinstance(by, str) and by else by or []

    s , a = _topn(df, c, _gcols, n)
    r = a.select(*_gcols, c, cnt).groupby(*_gcols).agg(F.collect_list(F.col(c)).alias(result))

    # the following forces colname to be nullable
    r = r.withColumn(index, F.udf(lambda x: x, T.StringType())(F.lit(c)))

    return r


def topn(df, c, by=None, n=3, others=False, index='_idx', result='_res'):
    """

    :param df: input dataframe
    :param c: column to aggregate
    :param by: group by
    :param n: top n results
    :param others: If true, it must be of the same type of the column being aggregated e.g 'others' for strings and -1 for positive numbers
    :param index:
    :param result:
    :return:
    """
    cnt = f'{c}##cnt'
    cnt_tot = f'{c}##tot'
    cnt_top = f'{c}##top'

    _gcols = [by] if isinstance(by, str) and by else by or []
    s , a = _topn(df, c, _gcols, n)
    r = a.select(*_gcols, c, cnt)

    if others:
        # colculate total topn and total
        o = a.groupby(_gcols).agg(F.sum(cnt).alias(cnt_top))
        t = s.groupby(*_gcols).agg(F.sum(cnt).alias(cnt_tot))

        # corner case single row
        o = o.join(t, on=_gcols) if _gcols else o.withColumn(cnt_tot, F.lit(t.take(1)[0][0]))

        o = o.select(
            *_gcols,
            F.lit(others).alias(c),
            (F.col(cnt_tot) - F.col(cnt_top)).alias(cnt)
        )

        # collect to list
        r = r.union(o)

    r = r.groupby(
        *_gcols
    ).agg(
        F.map_from_arrays(F.collect_list(F.col(c)), F.collect_list(F.col(cnt))
                          ).alias(result))

    # the following forces colname to be nullable
    r = r.withColumn(index, F.udf(lambda x: x, T.StringType())(F.lit(c)))

    return r


def percentiles(df, c, by=None, p=[10, 25, 50, 75, 90], index='_idx', result='_res'):
    _gcols = [by] if isinstance(by, str) and by else by or []
    ptile = f'{c}##p'

    # percentiles per row
    w = Window.partitionBy(*_gcols).orderBy(c)
    d = df.select(c, *_gcols, F.floor(100 * (F.percent_rank().over(w))).alias(ptile))

    # aggregate
    agg_keys = F.array(*[F.lit(x) for x in p])
    agg_values = F.array(*[F.max(F.when(F.col(ptile) < x, F.col(c))) for x in p])
    r = d.groupby(*_gcols).agg(F.map_from_arrays(agg_keys, agg_values).alias(result))

    # add colname
    r = r.withColumn(index, F.lit(c))

    return r
