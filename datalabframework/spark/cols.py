from datalabframework.decorators import add_attr
from pyspark.sql import DataFrame

def cols(self):
    return cols

DataFrame.cols = property(cols)