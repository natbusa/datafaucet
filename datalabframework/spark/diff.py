from pyspark.sql.types import  StringType
import pyspark.sql.functions as F

from datetime import datetime

def common_columns(df_a, df_b, exclude_cols=[]):
        colnames_a = set(df_a.columns)
        colnames_b = set(df_a.columns)
        colnames = colnames_a & colnames_b

        c = colnames.difference(set(exclude_cols))

        #preserve original order of the columns
        colnames_a = [x for x in df_a.columns if x in c]
        colnames_b = [x for x in df_b.columns if x in c]

        return colnames_a, colnames_b

def dataframe_diff(df_a, df_b, exclude_cols=[]):
    # This function will only produce DISTICT rows out!
    # Multiple exactly identical rows will be ingested only as one row

    # get columns
    colnames_a, colnames_b = common_columns(df_a, df_b, exclude_cols)

    # insert, modified
    df_a_min_b = df_a.select(colnames_a).subtract(df_b.select(colnames_b))

    # deleted
    df_b_min_a = df_b.select(colnames_b).subtract(df_a.select(colnames_a))

    return df_a_min_b, df_b_min_a

def dataframe_eventsource_view(df, state_col='_state', updated_col='_updated'):

    # calculate a view by :
    #   - squashing the events for each entry record to the last one
    #   - remove deleted record from the list

    c = set(df.columns).difference({state_col, updated_col})
    colnames = [x for x in df.columns if x in c]

    row_groups = df.groupBy(colnames)
    df_view = row_groups.agg(F.sort_array(F.collect_list(F.struct( F.col(updated_col), F.col(state_col))),asc = False).getItem(0).alias('_last')).select(*colnames, '_last.*')
    df = df_view.filter("{} = 0".format(state_col))

    return df

def dataframe_update(df_a, df_b=None, eventsourcing=False, exclude_cols=[], state_col='_state', updated_col='_updated'):

    df_b = df_b if df_b else df_a.filter("False")

    exclude_cols += [state_col, updated_col]
    colnames_a, colnames_b = common_columns(df_a, df_b, exclude_cols)

    if eventsourcing and (state_col in df_b.columns) and  (updated_col in df_b.columns) :
        df_b = dataframe_eventsource_view(df_b, state_col=state_col, updated_col=updated_col)
        df_upsert, df_delete = dataframe_diff(df_a, df_b, exclude_cols)
    else:
        df_upsert, df_delete = dataframe_diff(df_a, df_b, exclude_cols)
        df_delete = df_delete.filter("False")

    df_upsert = df_upsert.withColumn(state_col, F.lit(0))
    df_delete = df_delete.withColumn(state_col, F.lit(1))
    df_diff = df_upsert.union(df_delete)

    now = datetime.now()
    df_diff = df_diff.withColumn(updated_col, F.lit(now.strftime('%Y%m%dT%H%M%S')))

    # df_diff.show()
    return df_diff
