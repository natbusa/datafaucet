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
