import pandas as pd
import os

from pyspark.sql import types as T
from pyspark.sql import functions as F

import unidecode as ud

from faker import Faker
from numpy import random
import binascii
import zlib

import decimal
import datetime

from HLL import HyperLogLog
from datafaucet import crypto

def xor(s, t):
    tl = len(t)
    return bytes([s[i] ^ t[i%tl] for i in range(len(s))])

def xor_b64encode(data, key=None, encoding='utf-8', compressed=True):
    s = data.encode(encoding)
    key = key or str(len(s)**2)

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
    key = key or str(len(s)**2)
    t = key.encode(encoding)
    return xor(s,t).decode(encoding)

have_arrow = False
have_pandas = False

def _unidecode(s):
    return s if not s else ud.unidecode(s)

# Mapping Python types to Spark SQL DataType
python_type_mappings = {
    type(None): T.NullType,
    bool: T.BooleanType,
    int: T.LongType,
    float: T.DoubleType,
    str: T.StringType,
    bytearray: T.BinaryType,
    decimal.Decimal: T.DecimalType,
    datetime.date: T.DateType,
    datetime.datetime: T.TimestampType,
    datetime.time: T.TimestampType,
}

# Mapping string types to Spark SQL DataType
string_type_mapping = {
    'none': T.NullType,
    'null': T.NullType,
    'bool': T.BooleanType,
    'boolean': T.BooleanType,
    'int': T.IntegerType,
    'integer': T.IntegerType,
    'short': T.ShortType,
    'byte': T.ByteType,
    'long': T.LongType,
    'float': T.FloatType,
    'double': T.DoubleType,
    'date': T.DateType,
    'datetime': T.TimestampType,
    'time': T.TimestampType,
    'str': T.StringType,
    'string': T.StringType,
}

def get_type(obj):
    if obj is None:
        return T.NullType()

    if type(obj)==type(type):
        return python_type_mappings.get(obj)()

    if type(obj)==str:
        return string_type_mapping.get(obj)()

    raise TypeError('type ', type(obj), 'cannot be mapped')

def encrypt(key, encoding='utf-8'):
    @F.udf(T.StringType(), T.StringType())
    def _encrypt(data):
        fernet_func  = crypto.generate_fernet(key)
        s = data.encode(encoding)
        token = crypto.encrypt(s, fernet_func)
        r = binascii.b2a_base64(token, newline=False)
        return r.decode('ascii', 'ignore')
    return _encrypt

def decrypt(key, encoding='utf-8'):
    @F.udf(T.StringType(), T.StringType())
    def _decrypt(data):
        fernet_func  = crypto.generate_fernet(key)
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
        return xor_b64decode(data, key,encoding, compressed)
    return _unravel

def mask(s, e, c):
    @F.udf(T.StringType(), T.StringType())
    def _mask(d):
        return d[0:s]+len(d[s:e])*c+d[e:]
    return _mask


try:
    import pyarrow
    have_arrow = True

    if int(pyarrow.__version__.split('.')[1])>=15:
        os.environ['ARROW_PRE_0_15_IPC_FORMAT']='1'

    @F.pandas_udf(T.StringType(), F.PandasUDFType.SCALAR)
    def unidecode(series):
        return series.apply(lambda s: _unidecode(s))

    def fake(generator, *args, **kwargs):
        # run one sample to detect type for the udf
        d = getattr(Faker(), generator)(*args, **kwargs)
        @F.pandas_udf(get_type(type(d)), F.PandasUDFType.SCALAR)
        def fake_generator(series):
            fake = Faker()
            f = getattr(fake, generator)
            return series.apply(lambda s: f(*args, **kwargs))
        return fake_generator

    def randchoice(lst, p=None, seed=None, dtype=None):
        t = get_type(dtype) if dtype is not None else get_type(type(lst[0]))
        @F.pandas_udf(t, F.PandasUDFType.SCALAR)
        def choice_generator(series):
            return series.apply(lambda s: random.choice(lst, p).item())
        return choice_generator

    def hll_init(k=12):
        @F.pandas_udf(T.BinaryType(), F.PandasUDFType.SCALAR)
        def _hll_init(v):
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
        @F.pandas_udf(T.BinaryType(), F.PandasUDFType.GROUPED_AGG)
        def _hll_init_agg(v):
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
        @F.pandas_udf(T.LongType(), F.PandasUDFType.SCALAR)
        def _hll_count(v):
            hll = HyperLogLog(k)
            def count(hll, x):
                hll.set_registers(bytearray(x))
                return int(hll.cardinality())
            return v.apply(lambda x: count(hll, x))
        return _hll_count

    def hll_merge(k=12):
        @F.pandas_udf(T.BinaryType(), F.PandasUDFType.GROUPED_AGG)
        def _hll_merge(v):
            hll_res = HyperLogLog(k)
            hll = HyperLogLog(k)
            for x in v:
                hll.set_registers(bytearray(x))
                hll_res.merge(hll)
            return hll_res.registers()
        return _hll_merge

except ImportError:

    @F.udf(T.StringType(), T.StringType())
    def unidecode(s):
        return _unidecode(s)

    def fake(generator, *args, **kwargs):
        # run one sample to detect type for the udf
        d = getattr(Faker(), generator)(*args, **kwargs)

        @F.udf(get_type(type(d)), T.DataType())
        def fake_generator(s):
            fake = Faker()
            return getattr(fake, generator)(*args, **kwargs)
        return fake_generator

    def randchoice(lst, p=None, seed=None, dtype=None):
        t = get_type(dtype) if dtype is not None else get_type(type(lst[0]))
        @F.udf(t, T.DataType())
        def choice_generator(s):
            return random.choice(lst, p).item()
        return choice_generator

    def hll_init(k=12):
        raise NotImplementedError('Only available with PyArrow')
    def hll_init_agg(k=12):
        raise NotImplementedError('Only available with PyArrow')
    def hll_count(k=12):
        raise NotImplementedError('Only available with PyArrow')
    def hll_merge(k=12):
        raise NotImplementedError('Only available with PyArrow')
