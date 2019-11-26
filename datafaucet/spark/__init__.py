from pyspark.sql import DataFrame

from datafaucet.io import save
from datafaucet.decorators import add_method
from datafaucet.engines import register

import pyarrow
import os

if int(pyarrow.__version__.split('.')[1])>=15:
    os.environ['ARROW_PRE_0_15_IPC_FORMAT']='1'

# monkey patching: try to limit to a minimum

# 2 methods and 3 attributes in total
# df.save, df.datafaucet
# df.rows, df.cols, df.data

DataFrame.save = save

@add_method(DataFrame)
def datafaucet(self):
    return {
        'object': 'dataframe',
        'type': 'spark',
        'version':dfc.__version__
    }

from .rows import *
from .cols import *
from .data import *

# register the engine
from .engine import SparkEngine
register(SparkEngine, 'spark')
