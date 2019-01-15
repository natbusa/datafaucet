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

def add_datetime_columns(obj, column_name=None, tzone='GMT'):

    date_col = '_date'
    datetime_col = '_datetime'
    updated_col = '_updated'

    # add the _updated timestamp
    time_int = int(time.time())
    obj = obj.withColumn(updated_col, F.lit(time_int))

    if column_name in obj.columns:
        obj = obj.withColumn(datetime_col, to_timestamp(obj, column_name, tzone))
        obj = obj.withColumn(date_col, F.to_date(datetime_col))

    if date_col not in obj.columns:
        obj = obj.withColumn(date_col, F.lit('0001-01-01').cast(T.DateType()))

    if datetime_col not in obj.columns:
        obj = obj.withColumn(datetime_col, F.lit('0001-01-01').cast(T.TimestampType()))

    return obj