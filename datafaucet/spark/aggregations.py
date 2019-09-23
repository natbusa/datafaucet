import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

from datafaucet import logging
from datafaucet.spark import dataframe

class topn:
    def __init__(self, n = 3, others = None):
        self.n = n
        self.others = others
    
    def __call__(self, df, c, by=None):
        return dataframe.topn(df, c, by, self.n, self.others)

class topn_count:
    def __init__(self, n = 3):
        self.n = n
    
    def __call__(self, df, c, by=None):
        return dataframe.topn_count(df, c, by, self.n)

class percentiles:
    def __init__(self, p=[10, 25, 75, 90]):
        self.p = p
    
    def __call__(self, df, c, by=None):
        return dataframe.percentiles(df, c, by, self.p)
    
class typeof:
    def __call__(self, df, c, by=None):
        _gcols = [by] if isinstance(by, str) and by else by or [] 
        t = df.select(c).schema.fields[0].dataType
        return df.select(c, *_gcols).groupby(*_gcols).agg(F.lit(c).alias('colname'), F.lit(str(t)).alias('result'))

df_functions = (typeof, topn, topn_count, percentiles)

null = lambda c: F.sum(c.isNull().cast('int'))
nan = lambda c: F.sum(c.isnan)
integer = lambda c: F.coalesce(F.sum((F.rint(c) == c).cast('int')), F.lit(0))
boolean = lambda c: F.coalesce(F.sum((c.cast('boolean') == F.rint(c)).cast('int')), F.lit(0))
zero = lambda c: F.sum((c==0).cast('int'))
empty = lambda c: F.sum((F.length(c)==0).cast('int'))
pos = lambda c: F.sum((c>0).cast('int'))
neg = lambda c: F.sum((c<0).cast('int'))
distinct = lambda c: F.countDistinct(c)

count = F.count

sum = F.sum
sum_pos = lambda c: F.sum(F.when(c>0, c))
sum_neg = lambda c: F.sum(F.when(c<0, c))

min = F.min
max = F.max
avg = F.avg
stddev = F.stddev
skewness = F.skewness
kurtosis = F.kurtosis

digits_only = lambda c: F.sum((F.length(F.translate(c, '0123456789', ''))<F.length(c)).cast('int'))
spaces_only = lambda c: F.sum(((F.length(F.translate(c, ' \t', ''))==0) & (F.length(c)>0)).cast('int'))

all = {
    'type': typeof(),
    'integer': integer,
    'boolean': boolean,
    'top3': topn(),
    'percentiles': percentiles(),
    'null': null,
    'zero': zero,
    'empty': empty,
    'pos': pos,
    'neg':neg,
    'distinct': distinct,
    'sum':sum,
    'count':count,
    'min':min,
    'max':max,
    'avg': avg,
    'stddev':stddev,
    'skewness':skewness,
    'kurtosis':kurtosis,
    'sum_pos': sum_pos,
    'sum_neg': sum_neg,
    'digits_only': digits_only,
    'spaces_only': spaces_only,
}