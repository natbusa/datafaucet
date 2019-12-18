# Mapping Python types to Spark SQL DataType
from pyspark.sql import types as T

import decimal
import datetime

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

    if type(obj) == type(type):
        return python_type_mappings.get(obj)()

    if type(obj) == str:
        return string_type_mapping.get(obj)()

    raise TypeError('type ', type(obj), 'cannot be mapped')
