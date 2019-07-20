import pandas as pd

from pyspark.sql import types as T
from pyspark.sql import functions as F

import unidecode as ud

have_arrow = False
have_pandas = False

def _unidecode(s):
    return s if not s else ud.unidecode(s)

try:
    import pyarrow
    import pandas
    
    have_arrow = True
    have_pandas = True

    @F.pandas_udf(T.StringType(), F.PandasUDFType.SCALAR)
    def unidecode(series):
        return series.apply(_unidecode(s))
    
except ImportError:
    
    @F.udf(T.StringType(), T.StringType())
    def unidecode(s):
        return _unidecode(s)


def summary(df, cols):
        spark = df.sql_ctx
        types = {x.name:x.dataType for x in list(df.schema) if x.name in cols}
        
        res = pd.DataFrame.from_dict(types, orient='index')
        res.columns = ['datatype']
        
        count  = df.count()
        res['count'] = count

        d= df.select([F.approx_count_distinct(c).alias(c) for c in cols]).toPandas().T
        d.columns = ['approx_distinct']
        d.index.name = 'index'
        res = res.join(d)
        
        res['unique_ratio'] = res['approx_distinct']/count
        
        sel = []
        for c,v in types.items():
            if isinstance(v, (T.NumericType)):
                sel += [F.mean(c).alias(c)]
            else:
                sel += [F.min(F.lit(None)).alias(c)]
        d = df.select(sel).toPandas().T
        d.columns = ['mean']
        d.index.name = 'index'
        res = res.join(d)

        d= df.select([F.min(c).alias(c) for c in cols]).toPandas().T
        d.columns = ['min']
        d.index.name = 'index'
        res = res.join(d)

        d= df.select([F.max(c).alias(c) for c in cols]).toPandas().T
        d.columns = ['max']
        d.index.name = 'index'
        res = res.join(d)

        d= df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in cols]).toPandas().T
        d.columns = ['null']
        d.index.name = 'index'
        res = res.join(d)

        sel = []
        for c,v in types.items():
            if isinstance(v, (T.NumericType)):
                sel += [F.count(F.when(F.isnan(c), c)).alias(c)]
            else:
                sel += [F.min(F.lit(0)).alias(c)]
        d = df.select(sel).toPandas().T
        d.columns = ['nan']
        d.index.name = 'index'
        res = res.join(d)

        sel = []
        for c,v in types.items():
            if isinstance(v, (T.StringType)):
                sel += [F.count(F.when(F.col(c).isin(''), c)).alias(c)]
            else:
                sel += [F.min(F.lit(0)).alias(c)]
        d = df.select(sel).toPandas().T
        d.columns = ['empty']
        d.index.name = 'index'
        res = res.join(d)

        return res
