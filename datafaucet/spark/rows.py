import sys
from random import randint
from pyspark.sql import DataFrame

from datafaucet.spark import dataframe

INT_MAX = sys.maxsize
INT_MIN = -sys.maxsize-1

def sample(df, n=1000, *col, seed=None):
    # n 0<float<=1 -> fraction of samples
    # n floor(int)>1 -> number of samples

    # todo:
    # n dict of key, value pairs or array of (key, value)
    # cols = takes alist of columns for sampling if more than one column is provided
    # if a stratum is not specified, provide equally with what is left over form the total of the other quota

    if n>1:
        count = df.count()
        fraction = n/count
        return df if fraction>1 else df.sample(False, fraction, seed=seed)
    else:
        return df.sample(False, n, seed=seed)

_sample = sample

class Rows:
    def __init__(self, df, scols=None, gcols=None):
        self.df = df
        self.gcols = gcols or []

        self.scols = scols or df.columns
        self.scols = list(set(self.scols) - set(self.gcols))

    @property
    def columns(self):
        return [x for x in self.df.columns if x in (self.scols + self.gcols)]

    def overwrite(self,data):
        df = self.df
        return df.sql_ctx.createDataFrame(data,df.schema)

    def append(self, data):
        df = self.df
        return df.unionAll(df.sql_ctx.createDataFrame(data, df.schema))

    def sample(self, n=1000, *cols, random_state=True):
        return _sample(self.df, n, *cols, random_state)

    def pack(self, partition=1, bucket=1, order=None, sample=1.0):
        df = (self.df
                .select(self.columns)
                .repartition(partition)
                .orderBy(order))

        return _sample(df, s)

    def filter_by_date(self, column=None, start=None, end=None, window=None):
        df = dataframe.filter_by_datetime(self.df, column, start, end, window)
        return df

    def grid(self, limit=1000, render='qgrid'):
        try:
            from IPython.display import display
        except:
            display = None

        try:
            import qgrid
        except:
            render = 'default'
            logging.warning('Install qgrid for better visualisation. Using pandas as fallback.')

        # get the data
        data = self.df.select(self.columns).limit(limit).toPandas()

        if render=='qgrid':
            rendered = qgrid.show_grid(data)
        else:
            rendered = display(data) if display else data
        return rendered

    def one(self, as_type='pandas'):
        return self.collect(1, as_type=as_type)

    def collect(self, n, as_type='pandas'):
        return self.df.select(self.columns).limit(n).toPandas()

    @property
    def cols(self):
        from datafaucet.spark.cols import Cols
        return Cols(self.df, self.scols, self.gcols)

    @property
    def data(self):
        from datafaucet.spark.data import Data
        return Data(self.df, self.scols, self.gcols)


def _rows(self):
    return Rows(self)

DataFrame.rows = property(_rows)