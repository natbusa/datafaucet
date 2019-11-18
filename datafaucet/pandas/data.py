from pandas import DataFrame
from datafaucet.data import _Data

class Data(_Data):
    pass

def _data(self):
    return Data(self)

DataFrame.data = property(_data)
