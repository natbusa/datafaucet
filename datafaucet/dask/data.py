from dask import dataframe as dd
from datafaucet.data import _Data

class Data(_Data):

    def collect(self, n=1000, axis=0):
        #check if enough data in partition 0
        cnt = self.df.get_partition(0).count().compute().count()

        if cnt>n:
            res = self.df[self.columns].head(n)
        else:
            res = self.df[self.columns].compute().head(n)

        return res.T if axis else res

def _data(self):
    return Data(self)

(dd.DataFrame).data = property(_data)
