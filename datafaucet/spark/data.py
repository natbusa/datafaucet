from pyspark.sql import DataFrame
from datafaucet.data import _Data
from datafaucet.spark import dataframe


class Data(_Data):

    def collect(self, n=1000, axis=0):
        res = self.df.select(self.columns).limit(n).toPandas()
        return res.T if axis else res

    def scd_analyze(self, merge_on=None, **kwargs):
        return dataframe.scd_analyze(self.df, merge_on=merge_on, **kwargs).toPandas()


def _data(self):
    return Data(self)


DataFrame.data = property(_data)
