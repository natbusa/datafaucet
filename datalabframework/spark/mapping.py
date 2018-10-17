from datalabframework.spark.utils import remove_tones_udf
from pyspark.sql import types


def transform(df, mapping):
    # print("mapping", mapping)

    # drop columns
    drops = []
    for key, value in mapping.items():
        if value.get("drop", False):
            drops.append(key)

    # remove dropped fields from mapping list
    for key in drops:
        mapping.pop(key)

    df = df.drop(*drops)

    newMappings = {}
    # do the renaming first
    for key, value in mapping.items():
        currentname = key
        rename = value.get("name", None)
        if not (currentname in df.columns) or rename is None:  # new column or no rename
            newMappings[currentname] = value
            continue

        df = df.withColumnRenamed(currentname, rename)
        newMappings[rename] = value

    # do all remaining stuffs
    for key, value in newMappings.items():
        columnname = key
        isNew = not (columnname in df.columns)

        type = value.get("type", None)
        supportedTypes = [
            types.ByteType.typeName(),
            types.ShortType.typeName(),
            types.IntegerType.typeName(),
            types.LongType.typeName(),
            types.FloatType.typeName(),
            types.DoubleType.typeName(),
            types.DecimalType.typeName(),
            types.StringType.typeName(),
            types.BooleanType.typeName(),
            types.TimestampType.typeName(),
            types.DateType.typeName(),
            types.ArrayType.typeName(),
        ]
        # supported data types:
        # ['byte',
        #  'short',
        #  'integer',
        #  'long',
        #  'float',
        #  'double',
        #  'decimal',
        #  'string',
        #  'boolean',
        #  'timestamp',x
        #  'date',
        #  'array']
        from pyspark.sql import functions as F
        if (type is not None) and (not type in supportedTypes):
            raise ValueError("Type '%s' not supported!" % type)

        if isNew:  # new column
            columnValue = value.get("value", None)
            if columnValue is None:
                raise ValueError("`value` is required for new column `%s`" % columnname)
            df = df.withColumn(columnname, F.expr(columnValue))

        if type is not None:  # casting
            df = df.withColumn(columnname, F.col(columnname).cast(type))

        if value.get("remove_tones", False):
            if df.schema[columnname].dataType != types.StringType():
                raise ValueError("`remove_tones` option works for 'string' column only!")
            # print("Removed tones:", currentname)
            df = df.withColumn(columnname, remove_tones_udf(F.col(columnname)))

        fillna = value.get("fillna", None)  # fill should be passed to expr() function
        if fillna is not None:
            # print("fillna: ", columnname, fillna)
            # df = df.fillna(fillna, subset=[columnname])
            df = df.fillna({columnname: fillna})

    return df