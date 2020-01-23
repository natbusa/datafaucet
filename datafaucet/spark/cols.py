import functools

from pyspark.sql import DataFrame
from datafaucet import logging

import pyspark.sql.functions as F
import pyspark.sql.types as T

from datafaucet.spark import dataframe
from datafaucet.spark import functions
from datafaucet.spark import types

import re
import unidecode as ud

def to_dict(d=None):
    m = d or {}
    if isinstance(d, dict):
        m = m
    elif isinstance(d, (list, tuple)):
        m = {}
        for e in d:
            if isinstance(e, (list, tuple)):
                if len(e) > 1:
                    m[e[0]] = e[1]
                elif len(e) > 0:
                    m[e[0]] = e[0]
            elif isinstance(e, str):
                m[e] = e
    elif isinstance(d, str):
        m = dict(d, d)
    else:
        m = {}
    return m


class Cols:
    def __init__(self, df, scols=None, gcols=None):
        self.df = df
        self.gcols = gcols or []

        self.scols = scols or df.columns
        self.scols = [x for x in self.scols if x not in self.gcols]

    def _getcols(self, *colnames):
        return [x for x in colnames if x in self.df.columns]

    ## columns get/find/create/alias/drop
    @property
    def columns(self):
        return [x for x in self.df.columns if x in (self.scols + self.gcols)]

    def get(self, *colnames):
        self.scols = self._getcols(*colnames)
        return self

    def find(self, *by_regex, by_type=None, by_func=None):
        self.scols = dataframe.columns(self.df, *by_regex, by_type=by_type, by_func=by_func)
        return self

    def create(self, *colnames, dtype='string'):
        for c in colnames:
            self.df = self.df.withColumn(c, F.lit(None).cast(dtype))

        self.scols = self._getcols(*colnames)
        return self

    def alias(self, old, new):
        cols = self.df.columns
        idx = cols.index(old)
        a = cols[:idx + 1]
        b = cols[idx + 1:]
        self.df = self.df.select(*a, F.col(old).alias(new), *b)
        self.scols = [new]
        return self

    def drop(self, *cols):
        cols = self._getcols(*cols) if cols else self.scols

        df = self.df
        for c in cols:
            df = df.drop(c)
        return df

    def groupby(self, *colnames):
        # set group by list
        self.gcols = self._getcols(*colnames)

        # update select list
        self.scols = [x for x in self.scols if x not in self.gcols]
        return self

    @property
    def rows(self):
        from datafaucet.spark.rows import Rows
        return Rows(self.df, self.scols, self.gcols)

    @property
    def data(self):
        from datafaucet.spark.data import Data
        return Data(self.df, self.scols, self.gcols)

    ### actions
    def alter(self, *mods):
        df = self.df
        for c in self.scols:
            ci = c
            for t in mods:
                t = (t, None, None) if isinstance(t, str) else tuple(t)
                if t[0] == 'unidecode':
                    c = ud.unidecode(c)
                elif t[0] == 'alnum':
                    regex = re.compile('[^a-zA-Z0-9_]')
                    c = regex.sub('', c)
                elif t[0] == 'num':
                    regex = re.compile('[^0-9_]')
                    c = regex.sub('', c)
                elif t[0] == 'alpha':
                    regex = re.compile('[^a-zA-Z_]')
                    c = regex.sub('', c)
                elif t[0] == 'lower':
                    c = c.lower()
                elif t[0] == 'capitalize':
                    c = c.capitalize()
                elif t[0] == 'upper':
                    c = c.upper()
                elif t[0] == 'slice':
                    c = c[t[1]:t[2]] if t[2] else t[1]
                elif t[0] == 'translate':
                    c = c.translate(str.maketrans(t[1], t[2]))
                else:
                    pass
            df = df.withColumnRenamed(ci, c)
        return df

    def rename(self, mapping=None, target=None, prefix='', postfix=''):
        if not (mapping or target or prefix or postfix):
            return self.df

        if isinstance(mapping, str) and isinstance(target, str):
            old, new = (self._getcols(mapping)[0], target)
            return self.df.withColumnRenamed(old, prefix + new + postfix)

        if target:
            raise ValueError('mapping/target combination not allowed')

        # default to all columns
        d = dict(zip(self.scols, self.scols))

        # custom mapping
        if isinstance(mapping, str):
            colname = mapping
            m = [f'{colname}_{c}' for c in range(len(self.scols))]
            if len(self.scols) == 1:
                d = {self.scols[0]: colname}
            else:
                d = dict(zip(self.scols, m))

        if isinstance(mapping, (dict, list)):
            d = dict(mapping)

        # force colname to be in the list of available columns
        mapped_cols = set(self.scols) & set(d.keys())

        # renaming time!
        df = self.df
        for c in mapped_cols:
            df = df.withColumnRenamed(c, prefix + d[c] + postfix)

        return df

    def order(self, *cols, append_right=False):
        cols = self._getcols(*cols) or sorted(self.scols)
        ordered = [x for x in cols if x in self.scols]
        other = [x for x in self.scols if x not in cols]

        if append_right:
            return self.df.select(*other, *ordered)
        else:
            return self.df.select(*ordered, *other)

    def apply(self, f, prefix='', postfix=''):
        input_cols = self.scols
        output_cols = [f'{prefix}{c}{postfix}' for c in input_cols]

        df = self.df
        for ci, co in zip(input_cols, output_cols):
            df = df.withColumn(co, f(F.col(ci)))

        return df

    def expand(self, n, sep='_'):
        df = self.df
        for ci in self.scols:
            df = functions.expand(df, ci, n, sep)
        return df

    def cast(self, dtype):
        f = lambda c: c.cast(types.get_type(dtype))
        return self.apply(f)

    def tr(self, m, r):
        f = lambda c: F.translate(c, m, r)
        return self.apply(f)

    def mask(self, s, e, c):
        return self.apply(functions.mask(s, e, c))

    def randchoice(self, lst=[0, 1], p=None, seed=None, dtype=None):
        return self.apply(functions.randchoice(lst, p, seed, dtype))

    def randint(self, min=0, max=2, seed=None, dtype='int'):
        df = self.df
        for ci in self.scols:
            df = functions.randint(df, ci, min, max, seed, dtype)
        return df

    def randn(self, mu=0.0, sigma=1.0, seed=None):
        df = self.df
        for ci in self.scols:
            df = functions.randn(df, ci, mu, sigma, seed)
        return df

    def rand(self, min=0.0, max=1.0, seed=None):
        df = self.df
        for ci in self.scols:
            df = functions.rand(df, ci, min, max, seed)
        return df

    def fake(self, generator, *args, **kwargs):
        return self.apply(functions.fake(generator, *args, **kwargs))

    def hash(self, method='hash', preserve_type=True):
        f = {
            'crc32': F.crc32,
            'hash': F.hash
        }.get(method, F.hash)

        df = self.df
        cast = lambda ci: ci
        for c in self.scols:
            t = df.schema[c].dataType

            # keep the hash but preserve original type,
            # at the cost of more hash clashes (reduced hash space)

            # cast to string if method is crc32
            col = F.col(c).cast('string') if method == 'crc32' else F.col(c)

            if preserve_type and isinstance(t, (T.NumericType, T.StringType)):
                cast = lambda ci: ci.cast(t)

            df = df.withColumn(c, cast(f(col)))

        return df

    def hashstr(self, method='crc32'):
        f = {
            'crc32': F.crc32,
            'md5': F.md5,
            'md5-8': F.md5,
            'md5-4': F.md5,
            'sha1': F.sha1
        }.get(method, F.crc32)

        df = self.df
        for c in self.scols:
            col = F.col(c).cast('string')
            h = f(col)

            if method == 'crc32':
                res = F.conv(h.cast('string'), 10, 16)
            elif method == 'md5-8':
                res = F.substring(h, 0, 16)
            elif method == 'md5-4':
                res = F.substring(h, 0, 8)
            else:
                res = h

            df = df.withColumn(c, res)

        return df

    def hll_init(self, k=12):
        logging.warning("Consider using hll_init_agg instead: "
                        "ex: .groupby('g').agg(A.hll_init_agg())")
        return self.apply(functions.hll_init(k))

    def hll_count(self, k=12):
        return self.apply(functions.hll_count(k))

    def encrypt(self, key, encoding='utf-8'):
        return self.apply(functions.encrypt(key, encoding))

    def decrypt(self, key, encoding='utf-8'):
        return self.apply(functions.decrypt(key, encoding))

    def obscure(self, key=None, encoding='utf-8', compressed=True):
        return self.apply(functions.obscure(key, encoding, compressed))

    def unravel(self, key=None, encoding='utf-8', compressed=True):
        return self.apply(functions.unravel(key, encoding, compressed))

    def lower(self):
        return self.apply(F.lower)

    def upper(self):
        return self.apply(F.upper)

    def translate(self, matching=[], replace=[]):
        func = functools.partial(F.translate, matching, replace)
        return self.apply(func)

    def split(self, pattern):
        func = functools.partial(F.split, pattern=pattern)
        return self.apply(func)

    def unidecode(self, pre='', post=''):
        return self.apply(functions.unidecode, pre=pre, post=post)

    def summary(self):
        return functions.summary(self, self._cols)

    def agg(self, func, stack=None):
        # if stack is True, o:
        #  equivalent to pandas agg(...).stack(0)
        #  index is the name of the column to stack the original column names
        # if stack is false the column name is prepended to the aggregation name

        stack_col = stack if isinstance(stack, str) else '_idx'

        funcs = {}

        def agg2func(item):
            if isinstance(item, str):
                f = functions.all.get(item)
                f = f or functions.all_pandas_udf.get(item)
                if f:
                    return (item, f)
                else:
                    raise ValueError(f'function {item} not found')
            elif isinstance(item, (type(lambda x: x), type(max))):
                return item.__name__, item
            elif callable(item):
                return type(item).__name__, item
            else:
                raise ValueError('Invalid aggregation function')

        def parse_func(item):
            if isinstance(item, (tuple)):
                if len(item) == 2:
                    return item[0], agg2func(item[1])[1]
                else:
                    raise ValueError('Invalid list/tuple')
            else:
                return agg2func(item)

        def parse_list_func(func):
            func = [func] if type(func) != list else func
            return [parse_func(x) for x in func]

        def parse_dict_func(func):
            func = {0: func} if not isinstance(func, dict) else func
            return {x[0]: parse_list_func(x[1]) for x in func.items()}

        funcs = parse_dict_func(func)
        df = self.df

        def grouped(c):
            g = df.select(c, *self.gcols)
            return g if not self.gcols else g.groupby(*self.gcols)

        # either for all columns
        # or run specific functions for selected columns

        all_agg_cols = []
        for k, v in funcs.items():
            for (n, f) in v:
                if n not in all_agg_cols:
                    all_agg_cols.append(n)

        aggs = []
        for c in self.scols:

            # for this column
            agg_funcs = funcs.get(0, funcs.get(c))
            agg_cols = set([x[0] for x in agg_funcs])

            agg_dff = []
            groupeddata_funcs = []
            for agg_name in all_agg_cols:
                f = [f for (n, f) in agg_funcs if n == agg_name]
                f = f[0] if f else None
                if not f:
                    groupeddata_funcs.append(F.lit(None).alias(agg_name))
                else:
                    if not isinstance(f, functions.df_functions):
                        groupeddata_funcs.append(f(F.col(c)).alias(agg_name))
                    else:
                        agg_dff.append(f(df, c, by=self.gcols, index=stack_col, result=agg_name))

            agg_gdf = grouped(c).agg(F.lit(c).alias(stack_col), *groupeddata_funcs)

            if agg_dff:
                join_cols = self.gcols + [stack_col]
                dfs = [agg_gdf] + agg_dff
                agg = functools.reduce(lambda a, b: a.join(b, on=join_cols, how='outer'), dfs)
                agg = agg.select(*(join_cols + all_agg_cols))
            else:
                agg = agg_gdf

            aggs.append(agg)

        res = functools.reduce(lambda a, b: a.union(b), aggs)

        if not stack:
            all = [F.first(c).alias(f'{c}') for c in all_agg_cols]
            res = res.groupby(*self.gcols).pivot(stack_col).agg(*all)

        return res


def _cols(self):
    return Cols(self)


DataFrame.cols = property(_cols)
