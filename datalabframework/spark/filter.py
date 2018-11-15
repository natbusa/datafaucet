import pandas as pd
import dateutil.parser as dp

from datetime import date, datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T

import time

def to_timestamp(obj, column, tzone='GMT'):
    f = F.col(column)
    datecol_type = obj.select(column).dtypes[0][1]

    if datecol_type not in ['timestamp', 'date']:
        f = F.to_timestamp(f)

    if tzone != 'GMT':
        f = F.to_utc_timestamp(f, tzone)

    return f

def add_utctime_columns(obj, column, date_col, datetime_col, tzone):

    # if no date to extract, return just the _updated column
    if column not in obj.columns:
        return obj

    if datetime_col not in obj.columns:
        obj = obj.withColumn(datetime_col, to_timestamp(obj, column, tzone))

    if date_col not in obj.columns:
        obj = obj.withColumn(date_col, F.to_date(datetime_col))

    return obj


def add_reserved_columns(obj, options):

    state_col = '_state'
    date_col = '_date'
    datetime_col = '_datetime'
    updated_col = '_updated'

    # add the _updated timestamp
    obj = obj.withColumn(updated_col, F.lit(str(int(time.time()))))

    tzone =  options.get('tzone', 'GMT')
    column = options.get('date_column')
    obj = add_utctime_columns(obj,column, date_col, datetime_col,tzone)

    if date_col not in obj.columns:
        obj = obj.withColumn(date_col, F.lit('0001-01-01').cast(T.DateType()))

    if datetime_col not in obj.columns:
        obj = obj.withColumn(datetime_col, F.lit('0001-01-01').cast(T.TimestampType()))

    # fill with zeros if the following cols are not there
    if state_col not in obj.columns:
        obj = obj.withColumn(state_col, F.lit(0))

    return obj

def filter_by_date(obj, options, partition_date_column = None):

    if not obj:
        return None
    
    # defaults
    end_date_str = options.get('end_date')
    end_date = dp.isoparse(end_date_str) if end_date_str else datetime.now()

    window_str = options.get('window')
    window = pd.to_timedelta(window_str) if window_str else None

    start_date_str = options.get('start_date')
    start_date = dp.isoparse(start_date_str) if start_date_str else None

    tzone =  options.get('tzone', 'GMT')

    # default start-date from window and end_date
    if not start_date and window:
        start_date = end_date - window

    # not start, no end, means no filter necessary
    if not start_date and not end_date:
        return obj

    #  if date column != write partition colum use slow filter
    r_date_column = options.get('date', options.get('date_column'))
    w_date_column = partition_date_column

    # todo: write date column should come from schema ...
    # slow filter, using arbitrary column
    date_col = None
    datetime_col = r_date_column

    if r_date_column == w_date_column and '_date' in obj.columns and '_datetime' in obj.columns:
        # fast filter, using reserved column
        date_col = '_date'
        datetime_col = '_datetime'

    if date_col in obj.columns:
        obj = obj.filter(F.col(date_col) <= end_date.date())
        obj = obj.filter(F.col(date_col) >= start_date.date()) if start_date else obj

    if datetime_col in obj.columns:
        c = to_timestamp(obj, datetime_col, tzone)
        obj = obj.filter(c < end_date)
        obj = obj.filter(c >= start_date) if start_date else obj

    return obj


def transform(obj, settings, access):
    # 1. Get ingress policy
    policy = settings.get(access, {}).get('filter', {}).get('policy')

    if policy == 'date':
        partition_date_column = settings.get('write', {}).get('date_column')
        options = settings.get(access, {}).get('filter', {})
        obj = filter_by_date(obj, options, partition_date_column)

    return obj
