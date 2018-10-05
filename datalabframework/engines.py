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

    def read(self, resource, provider=None, **kargs):
        md = data.metadata(resource, provider)
        if not md:
            print('no valid resource found')
            return

        pd = md['provider']

        # override metadata with option specified on the read method
        options = utils.merge(md.get('options',{}), kargs)

        if pd['service'] == 'local':
            root = pd.get('path',project.rootpath())
            root = root if root[0]=='/' else '{}/{}'.format(project.rootpath(), root)
            url = "file://{}/{}".format(root, md['path'])
            url = url.translate(str.maketrans({"{":  r"\{","}":  r"\}"}))
            print(url)
            if pd['format']=='csv':
                return self._ctx.read.csv(url, **options)
            if pd['format']=='json':
                return self._ctx.read.option('multiLine',True).json(url, **options)
            if pd['format']=='jsonl':
                return self._ctx.read.json(url, **options)
            elif pd['format']=='parquet':
                return self._ctx.read.parquet(url, **options)
        elif pd['service'] == 'hdfs':
            url = "hdfs://{}:{}/{}/{}".format(pd['hostname'],pd.get('port', '8020'),pd['path'],md['path'])
            print(url)
            if pd['format']=='csv':
                return self._ctx.read.csv(url, **options)
            if pd['format']=='json':
                return self._ctx.read.option('multiLine',True).json(url, **options)
            if pd['format']=='jsonl':
                return self._ctx.read.json(url, **options)
            elif pd['format']=='parquet':
                return self._ctx.read.parquet(url, **options)
        elif pd['service'] == 'minio':
            url = "s3a://{}".format(os.path.join(pd['path'],md['path']))
            print(url)
            if pd['format']=='csv':
                return self._ctx.read.csv(url, **options)
            if pd['format']=='json':
                return self._ctx.read.option('multiLine',True).json(url, **options)
            if pd['format']=='jsonl':
                return self._ctx.read.json(url, **options)
            elif pd['format']=='parquet':
                return self._ctx.read.parquet(url, **options)
        elif pd['service'] == 'sqlite':
            url = "jdbc:sqlite:" + pd['path']
            driver = "org.sqlite.JDBC"
            return self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .load(**options)
        elif pd['service'] == 'mysql':
            url = "jdbc:mysql://{}:{}/{}".format(pd['hostname'],pd.get('port', '3306'),pd['database'])
            print(url)
            driver = "com.mysql.jdbc.Driver"
            return self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        elif pd['service'] == 'postgres':
            url = "jdbc:postgresql://{}:{}/{}".format(pd['hostname'],pd.get('port', '5432'),pd['database'])
            print(url)
            driver = "org.postgresql.Driver"
            return self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        elif pd['service'] == 'mssql':
            url = "jdbc:sqlserver://{}:{};databaseName={}".format(pd['hostname'],pd.get('port', '1433'),pd['database'])
            print(url)
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            return self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['path']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        else:
            raise('downt know how to handle this')

    def write(self, obj, resource, provider=None, **kargs):
        md = data.metadata(resource, provider)
        if not md:
            print('no valid resource found')
            return

        pd = md['provider']

        # override metadata with option specified on the read method
        options = utils.merge(md.get('options',{}), kargs)

        if pd['service'] == 'local':
            root = pd.get('path',project.rootpath())
            root = root if root[0]=='/' else '{}/{}'.format(project.rootpath(), root)
            url = "file://{}/{}".format(root, md['path'])
            if pd['format']=='csv':
                return obj.write.csv(url, **options)
            if pd['format']=='json':
                return obj.write.option('multiLine',True).json(url, **options)
            if pd['format']=='jsonl':
                return obj.write.json(url, **options)
            elif pd['format']=='parquet':
                return obj.write.parquet(url, **options)
        elif pd['service'] == 'hdfs':
            url = "hdfs://{}:{}/{}/{}".format(pd['hostname'],pd.get('port', '8020'),pd['path'],md['path'])
            if pd['format']=='csv':
                return obj.write.csv(url, **options)
            if pd['format']=='json':
                return obj.write.option('multiLine',True).json(url, **options)
            if pd['format']=='jsonl':
                return obj.write.json(url, **options)
            elif pd['format']=='parquet':
                return obj.write.parquet(url, **options)
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

class PandasEngine():
    def __init__(self, name, config):
        import pandas as pd

        self._ctx = pd
        self.info = {'name': name, 'context':'pandas', 'config': config}

    def context(self):
        return self._ctx

    def read(self, resource, **kargs):
        uri = data.uri(resource)
        path = data.path(resource)
        md = data.metadata(resource)

        if md['format']=='csv':
            return self._ctx.read_csv(path, **kargs)
        elif md['format']=='parquet':
            return self._ctx.read_parquet(path, **kargs)
        else:
            raise('downt know how to handle this')

    def write(self, obj, resource, **kargs):
        uri = data.uri(resource)
        path = data.path(resource)
        md = data.metadata(resource)

        if md['format']=='csv':
            return obj.to_csv(path, **kargs)
        elif md['format']=='parquet':
            return obj.to_parquet(path, **kargs)
        else:
            raise('downt know how to handle this')

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

        if cn['context']=='pandas':
            engine = PandasEngine(name, config)
            engines[name] = engine

    return engine
