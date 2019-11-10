import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

from datafaucet import logging
from datafaucet.spark import dataframe
from datafaucet.spark import utils

class topn:
    def __init__(self, n = 3, others = None):
        self.n = n
        self.others = others

    def __call__(self, df, c, by=None, index='_idx', result='_res'):
        return dataframe.topn(df, c, by, self.n, self.others, index, result)

class topn_count:
    def __init__(self, n = 3):
        self.n = n

    def __call__(self, df, c, by=None, index='_idx', result='_res'):
        return dataframe.topn_count(df, c, by, self.n, index, result)

class topn_values:
    def __init__(self, n = 3):
        self.n = n

    def __call__(self, df, c, by=None, index='_idx', result='_res'):
        return dataframe.topn_values(df, c, by, self.n, index, result)

class percentiles:
    def __init__(self, p=[10, 25, 75, 90]):
        self.p = p

    def __call__(self, df, c, by=None, index='_idx', result='_res'):
        return dataframe.percentiles(df, c, by, self.p, index, result)

class typeof:
    def __call__(self, df, c, by=None, index='_idx', result='_res'):
        _gcols = [by] if isinstance(by, str) and by else by or []
        t = df.select(c).schema.fields[0].dataType.simpleString()
        return df.select(c, *_gcols).groupby(*_gcols).agg(F.lit(c).alias(index), F.lit(t).alias(result))

df_functions = (typeof, topn, topn_count, topn_values, percentiles)

null = lambda c: F.sum(c.isNull().cast('int'))
nan = lambda c: F.sum(c.isnan)
integer = lambda c: F.coalesce(F.sum((F.rint(c) == c).cast('int')), F.lit(0))
boolean = lambda c: F.coalesce(F.sum((c.cast('boolean') == F.rint(c)).cast('int')), F.lit(0))
zero = lambda c: F.sum((c==0).cast('int'))
empty = lambda c: F.sum((F.length(c)==0).cast('int'))
pos = lambda c: F.sum((c>0).cast('int'))
neg = lambda c: F.sum((c<0).cast('int'))
distinct = lambda c: F.countDistinct(c)

one = lambda c: F.first(c, False).cast(T.StringType())
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
first = F.first

digits_only = lambda c: F.sum((F.length(F.translate(c, '0123456789', ''))<F.length(c)).cast('int'))
spaces_only = lambda c: F.sum(((F.length(F.translate(c, ' \t', ''))==0) & (F.length(c)>0)).cast('int'))

all = {
    'type': typeof(),
    'integer': integer,
    'boolean': boolean,
    'top3': topn(),
    'top3_count': topn_count(),
    'top3_values': topn_values(),
    'percentiles': percentiles(),
    'null': null,
    'zero': zero,
    'empty': empty,
    'pos': pos,
    'neg':neg,
    'distinct': distinct,
    'sum':sum,
    'count':count,
    'first':first,
    'one':one,
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

try:
    import pyarrow

    hll_init_agg = utils.hll_init_agg
    hll_merge = utils.hll_merge

    all.update({
        # PyArrow only
        'hll_init_agg': hll_init_agg(),
        'hll_merge': hll_merge(),
    })
except:
    pass
