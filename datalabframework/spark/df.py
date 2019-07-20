import datalabframework as dlf
from datalabframework.decorators import add_method

from pyspark.sql import DataFrame

# monkey patching: try to limit to a minimum

# 2 methods and 3 attributes in total
# df.save, df.datalabframework
# df.rows, df.cols, df.data

DataFrame.save = dlf.io.save

@add_method(DataFrame)
def datalabframework(self):
    print('datalabframework', dlf.__version__)

from .rows import *
from .cols import *
from .data import *