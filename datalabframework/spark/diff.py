import pyspark.sql.functions as F
import pyspark.sql.types as T

from datalabframework import spark as sparkfun

def columns(df_a=None, df_b=None, exclude_cols=[]):
    colnames_a = set(df_a.columns if df_a else [])
    colnames_b = set(df_b.columns if df_b else [])
    colnames = colnames_a & colnames_b

    c = colnames.difference(set(exclude_cols))

    # prefer df_b column order
    cols =  df_b.columns if df_b else df_a.columns

    # return a common column list in the same order
    # as provided by df_b or df_a column method
    return [x for x in cols if x in c]

def schema_diff(df_a=None, df_b=None, exclude_cols=[]):
    if not df_a and not df_b:
        return True

    if not df_a or not df_b:
        return False

    colnames = common_columns(df_a, df_b, exclude_cols)
    return df_a[colnames].schema.json() != df_b[colnames].schema.json()

def dataframe_diff(df_a=None, df_b=None, exclude_cols=[]):
    # This function will only produce DISTICT rows out!
    # Multiple exactly identical rows will be ingested only as one row
    if not df_a and not df_b:
        return None

    # df_b is None -> df_b is an empty dataframe
    df_b = df_b if df_b else sparkfun.utils.empty_dataframe(df_a)

    # get columns
    colnames = common_columns(df_a, df_b, exclude_cols)

    # insert, modified
    df_a_min_b = df_a.select(colnames).subtract(df_b.select(colnames))

    # deleted
    df_b_min_a = df_b.select(colnames).subtract(df_a.select(colnames))

    return df_a_min_b, df_b_min_a

def dataframe_update(df_a=None, df_b=None, upsert=True, delete=False, exclude_cols=[], state_col='_state'):
    # This function will only produce DISTICT rows out!
    # Multiple exactly identical rows will be ingested only as one row

    if not df_a and not df_b:
        return None

    # df_b is None -> df_b is an empty dataframe
    df_b = df_b if df_b else sparkfun.utils.empty_dataframe(df_a)

    #only compare common columns
    colnames = common_columns(df_a, df_b, exclude_cols + [state_col])

    # df_diff is an empty dataframe
    schema = df_a.select(colnames).schema
    schema.add(state_col, T.IntegerType(), True)

    df_diff  = df_a.sql_ctx.createDataFrame([],schema=schema)

    if upsert:
        df_upsert = df_a.select(colnames).subtract(df_b.select(colnames))
        df_diff = df_diff.union(df_upsert.withColumn(state_col, F.lit(0)))

    if delete:
        df_delete = df_b.select(colnames).subtract(df_a.select(colnames))
        df_diff = df_diff.union(df_delete.withColumn(state_col, F.lit(1)))

    return df_diff
