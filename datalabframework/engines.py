import os

from . import params
from . import data
from . import utils
from . import project

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

        md = params.metadata()['providers']
        for v in md.values():
            if v['service'] == 'minio':
                conf.set("spark.hadoop.fs.s3a.endpoint", 'http://{}:{}'.format(v['hostname'],v.get('port',9000))) \
                    .set("spark.hadoop.fs.s3a.access.key", v['access']) \
                    .set("spark.hadoop.fs.s3a.secret.key", v['secret']) \
                    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .set("spark.hadoop.fs.s3a.path.style.access", True)
                break

        conf.setMaster(config.get('master', 'local[*]'))
        self._ctx = SQLContext(SparkContext(conf=conf))
        self.info = {'name': name, 'context':'spark', 'config': config}

    def context(self):
        return self._ctx

    def read(self, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)
        if not md:
            print('no valid resource found')
            return

        pd = md['provider']

        cache = pd.get('read',{}).get('cache', False)
        cache = md.get('read',{}).get('cache', cache)
        
        repartition = pd.get('read',{}).get('repartition', None)
        repartition = pd.get('read',{}).get('repartition', repartition)
        
        coalesce = pd.get('read',{}).get('coalesce', None)
        coalesce = md.get('read',{}).get('coalesce', coalesce)

        print('repartition ', repartition)
        print('coalesce ', coalesce)
        print('cache', cache)
        
        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pd.get('read',{}).get('options',{}), md.get('read',{}).get('options',{}))
        options = utils.merge(options, kargs)

        if pd['service'] in ['local', 'hdfs', 'minio']:

            if pd['service'] == 'local':
                root = pd.get('path',project.rootpath())
                root = root if root[0]=='/' else '{}/{}'.format(project.rootpath(), root)
                url = "file://{}/{}".format(root, md['path'])
                url = url.translate(str.maketrans({"{":  r"\{","}":  r"\}"}))
            elif pd['service'] == 'hdfs':
                url = "hdfs://{}:{}/{}/{}".format(pd['hostname'],pd.get('port', '8020'),pd['path'],md['path'])
            elif pd['service'] == 'minio':
                url = "s3a://{}".format(os.path.join(pd['path'],md['path']))
            else:
                print('format unknown')
                return None
            
            print(url)
            if pd['format']=='csv':
                obj= self._ctx.read.csv(url, **options)
            if pd['format']=='json':
                obj= self._ctx.read.option('multiLine',True).json(url, **options)
            if pd['format']=='jsonl':
                obj= self._ctx.read.json(url, **options)
            elif pd['format']=='parquet':
                obj= self._ctx.read.parquet(url, **options)
        
        elif pd['service'] == 'sqlite':
            url = "jdbc:sqlite:" + pd['path']
            driver = "org.sqlite.JDBC"
            obj =  self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .load(**options)
        elif pd['service'] == 'mysql':
            url = "jdbc:mysql://{}:{}/{}".format(pd['hostname'],pd.get('port', '3306'),pd['database'])
            print(url)
            driver = "com.mysql.jdbc.Driver"
            obj =  self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        elif pd['service'] == 'postgres':
            url = "jdbc:postgresql://{}:{}/{}".format(pd['hostname'],pd.get('port', '5432'),pd['database'])
            print(url)
            driver = "org.postgresql.Driver"
            obj =  self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        elif pd['service'] == 'mssql':
            url = "jdbc:sqlserver://{}:{};databaseName={}".format(pd['hostname'],pd.get('port', '1433'),pd['database'])
            print(url)
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            obj = self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        else:
            raise('downt know how to handle this')
        
        obj = obj.repartition(repartition) if repartition else obj
        obj = obj.coalesce(coalesce) if coalesce else obj
        obj = obj.cache() if cache else obj

        return obj

    def write(self, obj, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)
        if not md:
            print('no valid resource found')
            return

        pd = md['provider']
        
        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pd.get('write',{}).get('options',{}), md.get('write',{}).get('options',{}))
        options = utils.merge(options, kargs)

        cache = pd.get('write',{}).get('cache', False)
        cache = md.get('write',{}).get('cache', cache)
        
        repartition = pd.get('write',{}).get('repartition', None)
        repartition = pd.get('write',{}).get('repartition', repartition)
        
        coalesce = pd.get('write',{}).get('coalesce', None)
        coalesce = md.get('write',{}).get('coalesce', coalesce)

        print('repartition ', repartition)
        print('coalesce ', coalesce)
        print('cache', cache)

        obj = obj.repartition(repartition) if repartition else obj
        obj = obj.coalesce(coalesce) if coalesce else obj
        obj = obj.cache() if cache else obj

        if pd['service'] in ['local', 'hdfs', 'minio']:
            if pd['service'] == 'local':
                root = pd.get('path',project.rootpath())
                root = root if root[0]=='/' else '{}/{}'.format(project.rootpath(), root)
                url = "file://{}/{}".format(root, md['path'])
            elif pd['service'] == 'hdfs':
                url = "hdfs://{}:{}/{}/{}".format(pd['hostname'],pd.get('port', '8020'),pd['path'],md['path'])
            elif pd['service'] == 'minio':
                url = "s3a://{}".format(os.path.join(pd['path'],md['path']))
            print(url)
                        
            if pd['format']=='csv':
                return obj.write.csv(url, **options)
            if pd['format']=='json':
                return obj.write.option('multiLine',True).json(url, **options)
            if pd['format']=='jsonl':
                return obj.write.json(url, **options)
            elif pd['format']=='parquet':
                return obj.write.parquet(url, **options)
            else:
                print('format unknown')
        elif pd['service'] == 'sqlite':
            url = "jdbc:sqlite:" + pd['path']
            driver = "org.sqlite.JDBC"
            return obj.write.format('jdbc').option('url', url)\
                      .option("dbtable", md['path']).option("driver", driver).save(**kargs)
        elif pd['service'] == 'mysql':
            url = "jdbc:mysql://{}:{}/{}".format(pd['hostname'],pd.get('port', '3306'),pd['database'])
            driver = "com.mysql.jdbc.Driver"
            return obj.write.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .save(**kargs)
        elif pd['service'] == 'postgres':
            url = "jdbc:postgresql://{}:{}/{}".format(pd['hostname'], pd.get('port', '5432'), pd['database'])
            print(url)
            driver = "org.postgresql.Driver"
            return obj.write.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .save(**kargs)
        else:
            raise('downt know how to handle this')


class ElasticEngine():
    def __init__(self, name, config):
        self.info = {'name': name, 'context':'elastic', 'config': config}

    def write(self, obj, resource=None, path=None, provider=None, mode='append',  **kargs):
        """
        :param mode: overwrite | append
        :param obj: spark dataframe, pandas dataframe, or list of Python dictionaries
        :param resource:
        :param path:
        :param provider:
        :param kargs:
        :return:
        """
        md = data.metadata(resource, path, provider)
        if not md:
            print('no valid resource found')
            return

        pd = md['provider']

        import elasticsearch
        import elasticsearch.helpers
        import json

        if pd["service"] == "elastic":
            uri = 'http://{}:{}'.format(pd["hostname"], pd["port"])
            es = elasticsearch.Elasticsearch([uri])
            if mode == "overwrite":
                if isinstance(md["mappings"]["properties"], str): # original properties JSON in Elastics format
                    # properties: >
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
                    md["mappings"]["properties"] = json.loads(md["mappings"]["properties"])
                else: # dictionary
                    #             properties:
                    #                 count: integer
                    #                 keyword: keyword
                    #                 query:
                    #                     type: text
                    #                     fields:
                    #                         keyword:
                    #                             type: keyword
                    #                             ignore_above: 256
                    #                 query_wo_tones:
                    #                     type: text
                    #                     fields:
                    #                         keyword:
                    #                             type: keyword
                    #                             ignore_above: 256
                    for k, v in md["mappings"]["properties"].items():
                        if isinstance(md["mappings"]["properties"][k], str):
                            md["mappings"]["properties"][k] = {"type":md["mappings"]["properties"][k]}

                    print(md["mappings"]["properties"])
                if not md["settings"] or not md["mappings"]:
                    raise ("'settings' and 'mappings' are required for 'overwrite' mode!")
                es.indices.delete(index=md["index"], ignore=404)
                es.indices.create(index=md["index"], body={
                    "settings": json.loads(md["settings"]),
                    "mappings": {
                        md["mappings"]["doc_type"]: {
                            "properties": md["mappings"]["properties"]
                        }
                    }
                })
            else: # append
                pass

            import pandas.core.frame
            import pyspark.sql.dataframe
            import numpy as np
            if isinstance(obj, pandas.core.frame.DataFrame):
                obj = obj.replace({np.nan:None}).to_dict(orient='records')
            elif isinstance(obj, pyspark.sql.dataframe.DataFrame):
                obj = obj.toPandas().replace({np.nan:None}).to_dict(orient='records')
            else: # python list of python dictionaries
                pass

            from collections import deque
            deque(elasticsearch.helpers.parallel_bulk(es, obj, index=md["index"],
                                                      doc_type=md["mappings"]["doc_type"]), maxlen=0)
            es.indices.refresh()
        else:
            raise ("Don't know how to handle this!")


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

        if cn['context']=='elastic':
            engine = ElasticEngine(name, config)
            engines[name] = engine

        if cn['context']=='pandas':
            engine = PandasEngine(name, config)
            engines[name] = engine

    return engine
