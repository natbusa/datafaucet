from datafaucet.engines import register
from .engine import SparkEngine

register(SparkEngine, 'spark')

# monkey patch the pyspark sql DataFrame
from .df import *
