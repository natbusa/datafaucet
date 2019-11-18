from datafaucet.engines import register
from .engine import DaskEngine

register(DaskEngine, 'dask')

# monkey patch the pyspark sql DataFrame
from .df import *
