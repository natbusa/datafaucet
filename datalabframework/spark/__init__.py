from datalabframework.engines import register
from .engine import SparkEngine

register(SparkEngine, 'spark')

# monkey patch the pyspark sql DataFrame
from .df import *
from .rows import *
from .cols import *