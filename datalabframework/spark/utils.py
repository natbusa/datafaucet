from pyspark.sql import types as T
from pyspark.sql import functions as F

import unidecode as ud

@F.pandas_udf(T.StringType(), F.PandasUDFType.SCALAR)
def unidecode(series):
    return series.apply(lambda s: ud.unidecode(s))

def empty_dataframe(df):
    return df.sql_ctx.createDataFrame([],df.schema)

