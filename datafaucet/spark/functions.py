import math

import pyspark
from pyspark.sql import functions as F
import pyspark.sql.types as T

from datafaucet import logging
from datafaucet.spark import utils

array_avg = F.udf( lambda x: sum(x)/len(x))
array_sum = F.udf( lambda x: sum(x))

def std(x, dof=1):
    avg = sum(x)/len(x)
    return math.sqrt(sum([(e -avg)**2 for e in x])/(len(x)- dof))

array_std = F.udf( lambda x: std(x))

def expand(df, c, n, sep='_'):
    t = df.schema[c].dataType

    if isinstance(t, T.ArrayType):
        ce = lambda i: F.col(c)[i]
    elif isinstance(t, T.MapType):
        ce = lambda i: F.map_values(c)[i]
    else:
        ce = lambda i: F.col(c)

    keys = n if isinstance(n, (list, tuple, range)) else range(n)
    sel = lambda c: [ce(i).alias(f'{c}{sep}{str(i)}') for i in keys]
    cols = [ sel(c) if x==c else [x] for x in df.columns ]
    cols = [item for sublist in cols for item in sublist]
    return df.select(*cols)

def rand(df, c, min=0.0, max=1.0, seed=None):
    range = max-min
    return df.withColumn(c, F.rand(seed)*range+min)

def randint(df, c, min=0, max=2, seed=None, dtype='int'):
    return rand(df, c, min, max, seed).withColumn(c, F.col(c).cast(dtype))

def randn(df, c, mu=0.0, sigma=1.0, seed=None):
    return df.withColumn(c, F.randn(seed)*sigma + mu)

def hll_init(k=12):
    return utils.hll_init(k)

def hll_count(k=12):
    return utils.hll_count(k)
