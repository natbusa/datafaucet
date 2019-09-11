import dataloof as dlf
from dataloof.decorators import add_method

from pyspark.sql import DataFrame

# monkey patching: try to limit to a minimum

# 2 methods and 3 attributes in total
# df.save, df.dataloof
# df.rows, df.cols, df.data

DataFrame.save = dlf.io.save

@add_method(DataFrame)
def dataloof(self):
    print('dataloof', dlf.__version__)

from .rows import *
from .cols import *
from .data import *