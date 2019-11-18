import datafaucet as dfc
from datafaucet.decorators import add_method

from dask import dataframe as dd

# monkey patching: try to limit to a minimum

# 2 methods and 3 attributes in total
# df.save, df.datafaucet
# df.rows, df.cols, df.data

(dd.DataFrame).save = dfc.io.save

@add_method(dd.DataFrame)
def datafaucet(self):
    return {'object': 'dataframe', 'type': 'dask', 'version':dfc.__version__}

from .rows import *
from .cols import *
from .data import *
