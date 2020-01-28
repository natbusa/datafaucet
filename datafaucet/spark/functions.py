import math

from pyspark.sql import functions as F
import pyspark.sql.types as T

import unidecode as ud

from faker import Faker
from numpy import random
import binascii
import zlib

from HLL import HyperLogLog
from datafaucet import crypto

from datafaucet.spark import types
from datafaucet.spark import dataframe

array_avg = F.udf(lambda x: sum(x) / len(x))
array_sum = F.udf(lambda x: sum(x))

import pandas as pd

def std(x, dof=1):
    avg = sum(x) / len(x)
    return math.sqrt(sum([(e - avg) ** 2 for e in x]) / (len(x) - dof))


array_std = F.udf(lambda x: std(x))


def expand(df, c, n, sep='_'):
    t = df.schema[c].dataType

    if isinstance(t, T.ArrayType):
        ce = lambda i: F.col(c)[i]
    elif isinstance(t, T.MapType):
        ce = lambda i: F.map_values(c)[i]
    else:
        ce = lambda i: F.col(c)

    if isinstance(n, (list, tuple, range)):
        sel = lambda x: [ce(i).alias(f'{str(n[i])}') for i in range(len(n))]
    else:
        sel = lambda x: [ce(i).alias(f'{x}{sep}{str(i)}') for i in range(n)]

    cols = [sel(c) if x == c else [x] for x in df.columns]
    cols = [item for sublist in cols for item in sublist]
    return df.select(*cols)


def rand(df, c, min=0.0, max=1.0, seed=None):
    range = max - min
    return df.withColumn(c, F.rand(seed) * range + min)


def randint(df, c, min=0, max=2, seed=None, dtype='int'):
    return rand(df, c, min, max, seed).withColumn(c, F.col(c).cast(dtype))


def randn(df, c, mu=0.0, sigma=1.0, seed=None):
    return df.withColumn(c, F.randn(seed) * sigma + mu)


def xor(s, t):
    tl = len(t)
    return bytes([s[i] ^ t[i % tl] for i in range(len(s))])


def xor_b64encode(data, key=None, encoding='utf-8', compressed=True):
    s = data.encode(encoding)
    key = key or str(len(s) ** 2)

    t = key.encode(encoding)
    r = xor(s, t)
    if compressed:
        c = zlib.compressobj(wbits=-15)
        c.compress(r)
        r = c.flush()
    r = binascii.b2a_base64(r, newline=False)
    return r.decode('ascii', 'ignore')


def xor_b64decode(data, key=None, encoding='utf-8', compressed=True):
    s = binascii.a2b_base64(data)
    if compressed:
        c = zlib.decompressobj(wbits=-15)
        s = c.decompress(s)
    key = key or str(len(s) ** 2)
    t = key.encode(encoding)
    return xor(s, t).decode(encoding)


def _unidecode(s):
    return s if not s else ud.unidecode(s)


def encrypt(key, encoding='utf-8'):
    @F.udf(T.StringType(), T.StringType())
    def _encrypt(data):
        fernet_func = crypto.generate_fernet(key)
        s = data.encode(encoding)
        token = crypto.encrypt(s, fernet_func)
        r = binascii.b2a_base64(token, newline=False)
        return r.decode('ascii', 'ignore')

    return _encrypt

def decrypt(key, encoding='utf-8'):
    @F.udf(T.StringType(), T.StringType())
    def _decrypt(data):
        fernet_func = crypto.generate_fernet(key)
        s = binascii.a2b_base64(data)
        msg = crypto.decrypt(s, fernet_func)
        return msg.decode(encoding)

    return _decrypt


def obscure(key=None, encoding='utf-8', compressed=True):
    @F.udf(T.StringType(), T.StringType())
    def _obscure(data):
        return xor_b64encode(data, key, encoding, compressed)

    return _obscure


def unravel(key=None, encoding='utf-8', compressed=True):
    @F.udf(T.StringType(), T.StringType())
    def _unravel(data):
        return xor_b64decode(data, key, encoding, compressed)

    return _unravel


def mask(s, e, c):
    @F.udf(T.StringType(), T.StringType())
    def _mask(d):
        return d[0:s] + len(d[s:e]) * c + d[e:]

    return _mask


@F.pandas_udf(T.StringType())
def unidecode(series: pd.Series) -> pd.Series:
    return series.apply(lambda s: _unidecode(s))


def fake(generator, *args, **kwargs):
    # run one sample to detect type for the udf
    d = getattr(Faker(), generator)(*args, **kwargs)

    @F.pandas_udf(types.get_type(type(d)))
    def fake_generator(series: pd.Series) -> pd.Series:
        fake = Faker()
        f = getattr(fake, generator)
        return series.apply(lambda s: f(*args, **kwargs))

    return fake_generator


def randchoice(lst, p=None, seed=None, dtype=None):
    t = types.get_type(dtype) if dtype is not None else types.get_type(type(lst[0]))

    @F.pandas_udf(t)
    def choice_generator(series: pd.Series) -> pd.Series:
        return series.apply(lambda s: random.choice(lst, p).item())

    return choice_generator


def hll_init(k=12):
    @F.pandas_udf(T.BinaryType())
    def _hll_init(v: pd.Series) -> pd.Series:
        hll = HyperLogLog(k)
        zero = hll.registers()

        def regs(x):
            hll.set_registers(zero);
            if x is not None:
                hll.add(str(x));
            return hll.registers()

        return v.apply(lambda x: regs(x))

    return _hll_init


def hll_init_agg(k=12):
    @F.pandas_udf(T.BinaryType())
    def _hll_init_agg(v: pd.DataFrame) -> bytes:
        hll_res = HyperLogLog(k)
        hll = HyperLogLog(k)
        for x in v:
            if isinstance(x, (bytes, bytearray)):
                hll.set_registers(bytearray(x))
                hll_res.merge(hll)
            elif x is not None:
                hll_res.add(str(x))
        return hll_res.registers()

    return _hll_init_agg


def hll_count(k=12):
    @F.pandas_udf(T.LongType())
    def _hll_count(v: pd.Series) -> pd.Series:
        hll = HyperLogLog(k)

        def count(hll, x):
            hll.set_registers(bytearray(x))
            return int(hll.cardinality())

        return v.apply(lambda x: count(hll, x))

    return _hll_count


def hll_merge(k=12):
    @F.pandas_udf(T.BinaryType())
    def _hll_merge(v: pd.DataFrame) -> bytes:
        hll_res = HyperLogLog(k)
        hll = HyperLogLog(k)
        for x in v:
            hll.set_registers(bytearray(x))
            hll_res.merge(hll)
        return hll_res.registers()

    return _hll_merge


class topn:
    def __init__(self, n=3, others=None):
        self.n = n
        self.others = others

    def __call__(self, df, c, by=None, index='_idx', result='_res'):
        return dataframe.topn(df, c, by, self.n, self.others, index, result)


class topn_count:
    def __init__(self, n=3):
        self.n = n

    def __call__(self, df, c, by=None, index='_idx', result='_res'):
        return dataframe.topn_count(df, c, by, self.n, index, result)


class topn_values:
    def __init__(self, n=3):
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
zero = lambda c: F.sum((c == 0).cast('int'))
empty = lambda c: F.sum((F.length(c) == 0).cast('int'))
pos = lambda c: F.sum((c > 0).cast('int'))
neg = lambda c: F.sum((c < 0).cast('int'))
distinct = lambda c: F.countDistinct(c)

one = lambda c: F.first(c, False).cast(T.StringType())
count = F.count

sum = F.sum
sum_pos = lambda c: F.sum(F.when(c > 0, c))
sum_neg = lambda c: F.sum(F.when(c < 0, c))

min = F.min
max = F.max
avg = F.avg
stddev = F.stddev
skewness = F.skewness
kurtosis = F.kurtosis
first = F.first

digits_only = lambda c: F.sum((F.length(F.translate(c, '0123456789', '')) < F.length(c)).cast('int'))
spaces_only = lambda c: F.sum(((F.length(F.translate(c, ' \t', '')) == 0) & (F.length(c) > 0)).cast('int'))

def strip(chars):
    return lambda c: lstrip(chars)(rstrip(chars)(c))

def rstrip(chars):
    return lambda c: F.regex_replace(c, f'[{chars}]*^', '')

def lstrip(chars):
    return lambda c: F.regex_replace(c, f'$[{chars}]*', '')

all = {
    'typeof': typeof(),
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
    'neg': neg,
    'distinct': distinct,
    'sum': sum,
    'count': count,
    'first': first,
    'one': one,
    'min': min,
    'max': max,
    'avg': avg,
    'stddev': stddev,
    'skewness': skewness,
    'kurtosis': kurtosis,
    'sum_pos': sum_pos,
    'sum_neg': sum_neg,
    'digits_only': digits_only,
    'spaces_only': spaces_only,
}

all_pandas_udf = {
    # PyArrow only
    'hll_init_agg': hll_init_agg(),
    'hll_merge': hll_merge(),
}
