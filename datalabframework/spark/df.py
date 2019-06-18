import datalabframework as dlf
from datalabframework.decorators import add_method

from pyspark.sql import DataFrame

DataFrame.save = dlf.io.save
DataFrame.save_csv = dlf.io.save_csv
DataFrame.save_json = dlf.io.save_json
DataFrame.save_parquet = dlf.io.save_parquet
DataFrame.save_jdbc = dlf.io.save_jdbc

@add_method(DataFrame)
def datalabframework(self):
    print('datalabframework', dlf.__version__)
