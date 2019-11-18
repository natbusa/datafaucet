from pyspark.sql import DataFrame
from datafaucet.data import _Data

class Data(_Data):

    def collect(self, n=1000, axis=0):
        res = self.df.select(self.columns).limit(n).toPandas()
        return res.T if axis else res

def _data(self):
    return Data(self)

DataFrame.data = property(_data)
