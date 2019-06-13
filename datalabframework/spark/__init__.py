from datalabframework.engines import register
from .engine import SparkEngine

register(SparkEngine, 'spark')