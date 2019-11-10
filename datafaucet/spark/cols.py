import functools

from pyspark.sql import DataFrame

from datafaucet import logging
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

from datafaucet.spark import dataframe
from datafaucet.spark import aggregations as A
from datafaucet.spark import utils
from datafaucet.spark import functions

def to_dict(d=None):
    m = d or {}
    if isinstance(d, dict):
        m = m
    elif isinstance(d, (list, tuple)):
        m = {}
        for e in d:
            if isinstance(e, (list, tuple)):
                if len(e)>1:
                    m[e[0]]=e[1]
                elif len(e)>0:
                    m[e[0]] = e[0]
            elif isinstance(e, str):
                m[e]=e
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

    @property
    def columns(self):
        return [x for x in self.df.columns if x in (self.scols + self.gcols)]

    def _getcols(self, *colnames):
        for c in set(colnames) - set(self.df.columns):
            logging.warning(f'Column not found: {c}')

        return [x for x in colnames if x in self.df.columns]

    def get(self, *colnames, sort=None):
        self.scols = self._getcols(*colnames)
        return self

    def find(self, *by_regex, by_type=None, by_func=None, sort=None):
        self.scols = dataframe.columns(self.df,*by_regex, by_type=by_type, by_func=by_func)
        return self

    def create(self, *colnames, as_type='string'):
        for c in colnames:
            self.df = self.df.withColumn(c, F.lit(None).cast(as_type))

        self.scols = self._getcols(*colnames)
        return self

    def groupby(self, *colnames):
        #set group by list
        self.gcols = self._getcols(*colnames)

        #update select list
        self.scols = [x for x in self.scols if x not in self.gcols]
        return self

    def rename(self, mapping=None, target=None, prefix='', postfix=''):
        # parsing
        if isinstance(mapping, str) and isinstance(target, str):
           d = {self._getcols(mapping)[0]:target}

        elif isinstance(mapping or target, str):
           colname = mapping or target
           m = [f'{colname}_{c}' for c in range(len(self.scols))]
           if len(self.scols)==1:
               d = {self.scols[0]:colname}
           else:
               d = dict(zip(self.scols, m))
        else:
            d = to_dict(mapping)

        # nothing to do if no rename
        if not d:
            return self.df

        mapped_cols = set(self.scols) & set(d.keys())

        df = self.df

        for c in mapped_cols:
            df = df.withColumnRenamed(c, prefix+d[c]+postfix)

        return df

    def order(self, *cols, where='start'):

        ordered = [x for x in cols if x in self.scols]
        other = [x for x in self.scols if x not in cols]

        return self.df.select(*ordered, *other)

    @property
    def rows(self):
        from datafaucet.spark.rows import Rows
        return Rows(self.df, self.scols, self.gcols)

    @property
    def data(self):
        from datafaucet.spark.data import Data
        return Data(self.df, self.scols, self.gcols)

    ### actions
    def drop(self, *colnames):
        df = self.df
        cols = self._getcols(*colnames) or self.scols
        for c in cols:
            df = df.drop(c)

        return df

    def dup(self, colnames):
        df = self.df
        cols = self._getcols(colnames)
        return self.df


    def apply(self, f, prefix='', postfix='', alias=None, rename=None):
        input_cols = self.scols
        output_cols = [f'{prefix}{c}{postfix}' for c in input_cols]

        if len(input_cols)==1 and alias:
            co = f'{prefix}{alias}{postfix}'
            ci = input_cols[0]
            df = self.df.withColumn(co, f(F.col(ci)))
        elif len(input_cols)==1 and rename:
            co = f'{prefix}{alias}{postfix}'
            ci = input_cols[0]
            df = self.df.withColumnRenamed(co, f(F.col(ci)))
        else:
            df = self.df
            for ci, co in zip(input_cols, output_cols):
                df = df.withColumn(co, f(F.col(ci)))

        return df

    def expand(self, n, sep='_'):
        df = self.df
        for ci in self.scols:
            df = functions.expand(df, ci, n, sep)
        return df

    def randchoice(self, lst=[0,1], p=None, seed=None, dtype=None):
        return self.apply(utils.randchoice(lst, p, seed, dtype))

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
        return self.apply(utils.fake(generator, *args, **kwargs))

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

            #cast to string if method is crc32
            col = F.col(c).cast('string') if method=='crc32' else F.col(c)

            if preserve_type and isinstance(t, (T.NumericType,  T.StringType)):
                cast = lambda ci: ci.cast(t)

            df = df.withColumn(c, cast(f(col)))

        return df

    def hashstr(self, method='crc32', salt=''):
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
            h = f(F.concat(col, F.lit(salt)))

            if method=='crc32':
                res = F.conv(h.cast('string'), 10, 16)
            elif method=='md5-8':
                res = F.substring(h, 0, 16)
            elif method=='md5-4':
                res = F.substring(h, 0, 8)
            else:
                res = h

            df = df.withColumn(c, res)

        return df

    def hll_init(self, k=12, alias=None):
        logging.warning("Consider using hll_init_agg instead: "
                        "ex: .groupby('g').agg(A.hll_init_agg())")
        return self.apply(functions.hll_init(k), alias=alias)

    def hll_count(self, k=12, alias=None, rename=None):
        return self.apply(functions.hll_count(k), alias=alias, rename=rename)

    def obscure(self, key=None, encoding='utf-8', alias=None):
        return self.apply(utils.obscure(key,encoding) , alias=alias)

    def unravel(self, key=None, encoding='utf-8', alias=None):
        return self.apply(utils.unravel(key,encoding), alias=alias)

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
        return self.apply(utils.unidecode, pre=pre, post=post)

    def one(self, as_type='pandas'):
        return self.collect(1, as_type=as_type)

    def collect(self, n, as_type='pandas'):
        return self.df.select(self.scols).limit(n).toPandas().T

    def summary(self):
        return utils.summary(self, self._cols)

    def agg(self, func, stack=None):
        # if stack is True, o:
        #  equivalent to pandas agg(...).stack(0)
        #  index is the name of the column to stack the original column names
        # if stack is false the column name is prepended to the aggregation name

        stack_col = stack if isinstance(stack, str) else '_idx'

        funcs = {}

        def string2func(func):
            if isinstance(func, str):
                f = A.all.get(func)
                if f:
                    return (func,f)
                else:
                    raise ValueError(f'function {func} not found')
            elif isinstance(func, (type(lambda x: x), type(max))):
                return (func.__name__, func)
            else:
                raise ValueError('Invalid aggregation function')

        def parse_single_func(func):
            if isinstance(func, (str, type(lambda x: x), type(max))):
                return string2func(func)
            elif isinstance(func, (tuple)):
                if len(func)==2:
                    return (func[0], string2func(func[1])[1])
                else:
                    raise ValueError('Invalid list/tuple')
            else:
                raise ValueError(f'Invalid aggregation item {func}')

        def parse_list_func(func):
            func = [func] if type(func)!=list else func
            return [parse_single_func(x) for x in func]

        def parse_dict_func(func):
            func = {0: func} if not isinstance(func, dict) else func
            return {x[0]:parse_list_func(x[1]) for x in func.items()}

        funcs  = parse_dict_func(func)
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
            agg_cols  = set([x[0] for x in agg_funcs])

            agg_dff = []
            groupeddata_funcs = []
            for agg_name in all_agg_cols:
                f = [f for (n,f) in agg_funcs if n == agg_name]
                f = f[0] if f else None
                if not f:
                    groupeddata_funcs.append(F.lit(None).alias(agg_name))
                else:
                    if not isinstance(f, A.df_functions):
                        groupeddata_funcs.append(f(F.col(c)).alias(agg_name))
                    else:
                        agg_dff.append(f(df, c, by=self.gcols, index=stack_col, result=agg_name))

            agg_gdf = grouped(c).agg(F.lit(c).alias(stack_col), *groupeddata_funcs)

            if agg_dff:
                join_cols = self.gcols + [stack_col]
                dfs  = [agg_gdf] + agg_dff
                agg = functools.reduce( lambda a, b: a.join(b, on=join_cols, how='outer'), dfs)
                agg = agg.select(*(join_cols+all_agg_cols))
            else:
                agg = agg_gdf

            aggs.append(agg)

        res= functools.reduce( lambda a, b: a.union(b), aggs)

        if not stack:
            all = [F.first(c).alias(f'{c}') for c in all_agg_cols]
            res = res.groupby(*self.gcols).pivot(stack_col).agg(*all)

        return res

def _cols(self):
    return Cols(self)

DataFrame.cols = property(_cols)
