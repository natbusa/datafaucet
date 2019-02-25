from pyspark.sql import types as T
from pyspark.sql import functions as F

import unidecode as ud

@F.pandas_udf(T.StringType(), F.PandasUDFType.SCALAR)
def unidecode(series):
    return series.apply(lambda s: s if not s else ud.unidecode(s))

