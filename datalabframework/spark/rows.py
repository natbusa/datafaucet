from datalabframework.decorators import add_attr
from pyspark.sql import DataFrame

def rows(self):
    
    @add_attr(rows)
    def overwrite(data):
        return self.sql_ctx.createDataFrame(data,self.schema)

    @add_attr(rows)
    def append(data):
        return self.unionAll(self.sql_ctx.createDataFrame(data,self.schema))

    return rows

DataFrame.rows = property(rows)