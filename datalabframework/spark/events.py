import pyspark.sql.functions as F
import pyspark.sql.types as T

def dataframe_view(df, state_col='_state', updated_col='_updated'):

    # calculate a view by :
    #   - squashing the events for each entry record to the last one
    #   - remove deleted record from the list

    c = set(df.columns).difference({state_col, updated_col})
    colnames = [x for x in df.columns if x in c]

    row_groups = df.groupBy(colnames)
    df_view = row_groups.agg(F.sort_array(F.collect_list(F.struct( F.col(updated_col), F.col(state_col))),asc = False).getItem(0).alias('_last')).select(*colnames, '_last.*')
    df = df_view.filter("{} = 0".format(state_col))

    return df


