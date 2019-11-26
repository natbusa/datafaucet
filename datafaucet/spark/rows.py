import sys
from random import randint
from pyspark.sql import DataFrame

from datafaucet.spark import dataframe

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
        return df.unionByName(df.sql_ctx.createDataFrame(data, df.schema))

    def sample(self, n=1000, *cols, random_state=True):
        return dataframe.sample(self.df, n, *cols, random_state)

    def filter_by_date(self, column=None, start=None, end=None, window=None):
        df = dataframe.filter_by_datetime(self.df, column, start, end, window)
        return df

    def filter(self, *args, **kwargs):
        return self.df.filter(*args, **kwargs)

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
