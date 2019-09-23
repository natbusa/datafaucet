import math

import pyspark
from pyspark.sql import functions as F
import pyspark.sql.types as T

from datafaucet import logging

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
    
