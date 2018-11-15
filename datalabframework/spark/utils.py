from pyspark.sql import types as T
from pyspark.sql import functions as F

def empty_dataframe(df):
    return df.sql_ctx.createDataFrame([],df.schema)

def remove_tones(s):
    itab = "àáảãạăắằẵặẳâầấậẫẩđèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵÀÁẢÃẠĂẮẰẴẶẲÂẦẤẬẪẨĐÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴ"
    otab = "aaaaaaaaaaaaaaaaadeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyAAAAAAAAAAAAAAAAADEEEEEEEEEEEIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYY"
    ttab = str.maketrans(itab, otab)
    return s if not s else s.translate(ttab).lower()


@F.pandas_udf(T.StringType(), F.PandasUDFType.SCALAR)
def remove_tones_udf(series):
    return series.apply(remove_tones)
