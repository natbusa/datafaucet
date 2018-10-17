import os

from jinja2 import Template
from pyspark.sql import types

from . import params
from . import data
from . import utils
from . import project
import elasticsearch.helpers
import json
import pyspark

from . import logging
logger = logging.getLogger()

# purpose of engines
# abstract engine init, data read and data write
# and move this information to metadata

# it does not make the code fully engine agnostic though.

engines = dict()

class SparkEngine():
    def __init__(self, name, config):
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SQLContext

        here = os.path.dirname(os.path.realpath(__file__))

        submit_args = ''

        jars = []
        jars += config.get('jars', [])
        if jars:
            submit_jars = ' '.join(jars)
            submit_args = '{} --jars {}'.format(submit_args, submit_jars)

        packages = config.get('packages', [])
        if packages:
            submit_packages = ','.join(packages)
            submit_args = '{} --packages {}'.format(submit_args, submit_packages)

        submit_args = '{} pyspark-shell'.format(submit_args)

        # os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.postgresql:postgresql:42.2.5 pyspark-shell"
        os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
        print('PYSPARK_SUBMIT_ARGS: {}'.format(submit_args))

        conf = SparkConf()
        if 'jobname' in config:
            conf.setAppName(config.get('jobname'))

        rmd = params.metadata()['providers']
        for v in rmd.values():
            if v['service'] == 'minio':
                conf.set("spark.hadoop.fs.s3a.endpoint", 'http://{}:{}'.format(v['hostname'],v.get('port',9000))) \
                    .set("spark.hadoop.fs.s3a.access.key", v['access']) \
                    .set("spark.hadoop.fs.s3a.secret.key", v['secret']) \
                    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .set("spark.hadoop.fs.s3a.path.style.access", True)
                break

        #set master
        conf.setMaster(config.get('master', 'local[*]'))

        #set log level fro spark
        sc = SparkContext(conf=conf)

        # pyspark set log level method
        # (this will not suppress WARN before starting the context)
        sc.setLogLevel("ERROR")

        self._ctx = SQLContext(sc)
        self.info = {'name': name, 'context':'spark', 'config': config}

    def context(self):
        return self._ctx

    def read(self, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)
        if not md:
            print('no valid resource found')
            return

        pmd = md['provider']
        rmd = md['resource']

        url = md['url']

        cache = pmd.get('read',{}).get('cache', False)
        cache = rmd.get('read',{}).get('cache', cache)

        repartition = pmd.get('read',{}).get('repartition', None)
        repartition = rmd.get('read',{}).get('repartition', repartition)

        coalesce = pmd.get('read',{}).get('coalesce', None)
        coalesce = rmd.get('read',{}).get('coalesce', coalesce)

        mapping = pmd.get('read', {}).get('mapping', None)
        mapping = rmd.get('read', {}).get('mapping', mapping)

        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pmd.get('read',{}).get('options',{}), rmd.get('read',{}).get('options',{}))
        options = utils.merge(options, kargs)

        if pmd['service'] in ['sqlite', 'mysql', 'postgres', 'mssql']:
            format = pmd.get('format', 'rdbms')
        else:
            format = pmd.get('format', 'parquet')

        if pmd['service'] in ['local', 'hdfs', 'minio']:
            if format=='csv':
                obj= self._ctx.read.csv(url, **options)
            if format=='json':
                obj= self._ctx.read.option('multiLine',True).json(url, **options)
            if format=='jsonl':
                obj= self._ctx.read.json(url, **options)
            elif format=='parquet':
                obj= self._ctx.read.parquet(url, **options)

        elif pmd['service'] == 'sqlite':
            driver = "org.sqlite.JDBC"
            obj =  self._ctx.read \
                .format('jdbc') \
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .load(**options)

        elif pmd['service'] == 'mysql':
            driver = "com.mysql.cj.jdbc.Driver"
            obj =  self._ctx.read\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .option("user",pmd['username'])\
                .option('password',pmd['password'])\
                .load(**options)
        elif pmd['service'] == 'postgres':
            driver = "org.postgresql.Driver"
            obj =  self._ctx.read\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .option("user",pmd['username'])\
                .option('password',pmd['password'])\
                .load(**options)
        elif pmd['service'] == 'mssql':
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            obj = self._ctx.read \
                .format('jdbc') \
                .option('url', url) \
                .option("dbtable", rmd['path']) \
                .option("driver", driver) \
                .option("user", pmd['username']) \
                .option('password', pmd['password']) \
                .load(**options)
        elif pmd['service'] == 'elastic':
            # uri = 'http://{}:{}/{}'.format(pmd["hostname"], pmd["port"], md['path'])
            # print(options)
            obj = self.elastic_read(url, options.get('query', {}))
        else:
            raise('downt know how to handle this')

        obj = self.transform(obj, mapping) if mapping else obj
        obj = obj.repartition(repartition) if repartition else obj
        obj = obj.coalesce(coalesce) if coalesce else obj
        obj = obj.cache() if cache else obj

        #logging
        logger.info({'format':format,'service':pmd['service'],'path':rmd['path'], 'url':md['url']}, extra={'dlf_type':'engine.read'})

        return obj

    def write(self, obj, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)
        if not md:
            print('no valid resource found')
            return

        pmd = md['provider']
        rmd = md['resource']

        url = md['url']

        cache = pmd.get('read',{}).get('cache', False)
        cache = rmd.get('read',{}).get('cache', cache)

        repartition = pmd.get('write',{}).get('repartition', None)
        repartition = rmd.get('write',{}).get('repartition', repartition)

        coalesce = pmd.get('write',{}).get('coalesce', None)
        coalesce = rmd.get('write',{}).get('coalesce', coalesce)

        mapping = pmd.get('read', {}).get('mapping', None)
        mapping = rmd.get('read', {}).get('mapping', mapping)

        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pmd.get('write',{}).get('options',{}), rmd.get('write',{}).get('options',{}))
        options = utils.merge(options, kargs)

        obj = self.transform(obj, mapping) if mapping else obj
        obj = obj.repartition(repartition) if repartition else obj
        obj = obj.coalesce(coalesce) if coalesce else obj
        obj = obj.cache() if cache else obj

        if pmd['service'] in ['sqlite', 'mysql', 'postgres', 'mssql']:
            format = pmd.get('format', 'rdbms')
        else:
            format = pmd.get('format', 'parquet')

        if pmd['service'] in ['local', 'hdfs', 'minio']:
            if format=='csv':
                obj.write.csv(url, **options)
            if format=='json':
                obj.write.option('multiLine',True).json(url, **options)
            if format=='jsonl':
                obj.write.json(url, **options)
            elif format=='parquet':
                obj.write.parquet(url, **options)
            else:
                print('format unknown')

        elif pmd['service'] == 'sqlite':
            driver = "org.sqlite.JDBC"
            obj.write\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .save(**kargs)

        elif pmd['service'] == 'mysql':
            driver = "com.mysql.cj.jdbc.Driver"
            obj.write\
                .format('jdbc')\
                .option('url', url)\
                .option("dbtable", rmd['path'])\
                .option("driver", driver)\
                .option("user",pmd['username'])\
                .option('password',pmd['password'])\
                .save(**kargs)

        elif pmd['service'] == 'postgres':
            driver = "org.postgresql.Driver"
            obj.write \
                .format('jdbc') \
                .option('url', url) \
                .option("dbtable", rmd['path']) \
                .option("driver", driver) \
                .option("user", pmd['username']) \
                .option('password', pmd['password']) \
                .save(**kargs)
        elif pmd['service'] == 'elastic':
            uri = 'http://{}:{}'.format(pmd["hostname"], pmd["port"])

            # print(options)
            # if "mode" in kargs and kargs.get("mode") == "overwrite":
            #     mode = "overwrite"
            # else:
            #     mode = "append"
            mode = kargs.get("mode", None)
            elastic_write(obj, uri, mode, rmd["path"], options["settings"], options["mappings"])
        else:
            raise('downt know how to handle this')

        logger.info({'format':format,'service':pmd['service'], 'path':rmd['path'], 'url':md['url']}, extra={'dlf_type':'engine.write'})

    def elastic_read(self, url, query):
        results = elastic_read(url, query)
        rows = [pyspark.sql.Row(**r) for r in results]
        return self.context().createDataFrame(rows)

    def transform(self, df, mapping):
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
            if not (currentname in df.columns) or rename is None: # new column or no rename
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

            if isNew : # new column
                columnValue = value.get("value", None)
                if columnValue is None:
                    raise ValueError("`value` is required for new column `%s`" % columnname)
                df = df.withColumn(columnname, F.expr(columnValue))

            if type is not None: # casting
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
                df = df.fillna({columnname:fillna})

        return df



def remove_tones(s):
    intab  = "àáảãạăắằẵặẳâầấậẫẩđèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵÀÁẢÃẠĂẮẰẴẶẲÂẦẤẬẪẨĐÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴ"
    outtab = "aaaaaaaaaaaaaaaaadeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyAAAAAAAAAAAAAAAAADEEEEEEEEEEEIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYY"
    # return s if not s else s.translate(str.maketrans(intab, outtab))
    return s if not s else s.translate(str.maketrans(intab, outtab)).lower()


from pyspark.sql.functions import pandas_udf, PandasUDFType


@pandas_udf(types.StringType(), PandasUDFType.SCALAR)
def remove_tones_udf(series):
    return series.apply(remove_tones)



def elastic_read(url, query):
    """
    :param url:
    :param query:
    :return: python list of dict

    sample resource:
    variables:
        size: 10
        from: 0
        query: "laptop"
    keywords_search:
        provider: elastic_test
        path: /search_keywords_dev/keyword/
        query: >
            {
              "size" : "{{ default.variables.size }}",
              "from" : "{{ default.variables.size }}",
              "query": {
                "function_score": {
                  "query": {
                      "match" : {
                        "query_wo_tones": {"query":"{{ default.variables.query }}", "operator" :"or", "fuzziness":"AUTO"}
                    }
                  },
                  "script_score" : {
                      "script" : {
                        "source": "Math.sqrt(doc['impressions'].value)"
                      }
                  },
                  "boost_mode":"multiply"
                }
              }
            }
    """

    try:
        es = elasticsearch.Elasticsearch([url])
        res = es.search(body=query)
    except:
        raise ValueError("Cannot query elastic search")

    hits = res.pop("hits", None)
    if hits is None:
        #handle ERROR
        raise ValueError("Query failed")

        # res["total_hits"] = hits["total"]
        # res["max_score"] = hits["max_score"]
        # print("Summary:", res)

    hits2 = []
    for hit in hits['hits']:
        hitresult = hit.pop("_source", None)
        hits2.append({**hit, **hitresult})

    return hits2


def elastic_write(obj, uri, mode='append', indexName=None, settings=None, mappings=None):
    """
    :param mode: overwrite | append
    :param obj: spark dataframe, or list of Python dictionaries
    :return:
    """
    es = elasticsearch.Elasticsearch([uri])
    if mode == "overwrite":
            # properties:
            #                 {
            #                     "count": {
            #                         "type": "integer"
            #                     },
            #                     "keyword": {
            #                         "type": "keyword"
            #                     },
            #                     "query": {
            #                         "type": "text",
            #                         "fields": {
            #                             "keyword": {
            #                                 "type": "keyword",
            #                                 "ignore_above": 256
            #                             }
            #                         }
            #                     }
            #                 }
            # OR:
            # properties:
            #     count: integer
            #     keyword: keyword
            #     query:
            #         type: text
            #         fields:
            #             keyword:
            #                 type: keyword
            #                 ignore_above: 256
            #     query_wo_tones:
            #         type: text
            #         fields:
            #             keyword:
            #                 type: keyword
            #                 ignore_above: 256
        for k, v in mappings["properties"].items():
            if isinstance(mappings["properties"][k], str):
                mappings["properties"][k] = {"type":mappings["properties"][k]}
            # settings:
            #         {
            #             "index": {
            #                 "number_of_shards": 1,
            #                 "number_of_replicas": 3,
            #                 "mapping": {
            #                     "total_fields": {
            #                         "limit": "1024"
            #                     }
            #                 }
            #             }
            #         }
            # OR:
            # settings:
            #     index:
            #         number_of_shards: 1
            #         number_of_replicas: 3
            #         mapping:
            #             total_fields:
            #                 limit: 1024

        print(settings)
        print(mappings["properties"])

        if not settings or not settings:
            raise ("'settings' and 'mappings' are required for 'overwrite' mode!")
        es.indices.delete(index=indexName, ignore=404)
        es.indices.create(index=indexName, body={
            "settings": settings,
            "mappings": {
                mappings["doc_type"]: {
                    "properties": mappings["properties"]
                }
            }
        })
    elif mode == "append": # append
        pass
    else:
        raise ValueError("Unsupported mode: " + mode)

    # import pandas.core.frame
    # import pyspark.sql.dataframe
    # import numpy as np
    # if isinstance(obj, pandas.core.frame.DataFrame):
    #     obj = obj.replace({np.nan:None}).to_dict(orient='records')
    # elif isinstance(obj, pyspark.sql.dataframe.DataFrame):
    #     obj = obj.toPandas().replace({np.nan:None}).to_dict(orient='records')
    # else: # python list of python dictionaries
    #     pass

    if isinstance(obj, pyspark.sql.dataframe.DataFrame):
        obj = [row.asDict() for row in obj.collect()]
    else: # python list of python dictionaries
        pass

    from collections import deque
    deque(elasticsearch.helpers.parallel_bulk(es, obj, index=indexName,
                                              doc_type=mappings["doc_type"]), maxlen=0)
    es.indices.refresh()


def get(name):
    global engines

    #get
    engine = engines.get(name)

    if not engine:
        #create
        md = params.metadata()
        cn = md['engines'].get(name)
        config = cn.get('config', {})

        if cn['context']=='spark':
            engine = SparkEngine(name, config)
            engines[name] = engine

    return engine
