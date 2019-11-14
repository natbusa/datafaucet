from datafaucet.engines import register
from .engine import PandasEngine

register(PandasEngine, 'pandas')

# monkey patch the pyspark sql DataFrame
from .df import *
