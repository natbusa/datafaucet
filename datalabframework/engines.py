import os

from . import params
from . import data
from . import utils

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

        os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
        print(submit_args)

        conf = SparkConf()
        if 'jobname' in config:
            conf.setAppName(config.get('jobname'))

        conf.setMaster(config.get('master', 'local[*]'))
        self._ctx = SQLContext(SparkContext(conf=conf))
        self.info = {'name': name, 'context':'spark', 'config': config}

    def context(self):
        return self._ctx

    def read(self, resource, **kargs):
        path = data.path(resource)
        md = data.metadata(resource)

        #reference provider from data alias
        pd = params.metadata()['providers'][md['provider']]

        # override metadata with option specified on the read method
        options = utils.merge(md.get('options',{}), kargs)

        if pd['service'] == 'fs':
            if pd['format']=='csv':
                return self._ctx.read.csv(path, **options)
            if pd['format']=='json':
                return self._ctx.read.option('multiLine',True).json(path, **options)
            if pd['format']=='jsonl':
                return self._ctx.read.json(path, **options)
            elif pd['format']=='parquet':
                return self._ctx.read.parquet(path, **options)
        elif pd['service'] == 'sqlite':
            url = "jdbc:sqlite:" + pd['path']
            driver = "org.sqlite.JDBC"
            return self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['table']).option("driver", driver)\
                   .load(**options)
        elif pd['service'] == 'mysql':
            url = "jdbc:mysql://{}:{}/{}".format(pd['hostname'],pd.get('port', '3306'),pd['database'])
            print(url)
            driver = "com.mysql.jdbc.Driver"
            return self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['table']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        elif pd['service'] == 'postgres':
            url = "jdbc:postgresql://{}:{}/{}".format(pd['hostname'],pd.get('port', '5432'),pd['database'])
            print(url)
            driver = "org.postgresql.Driver"
            return self._ctx.read.format('jdbc').option('url', url)\
                   .option("dbtable", md['table']).option("driver", driver)\
                   .option("user",pd['username']).option('password',pd['password'])\
                   .load(**options)
        else:
            raise('downt know how to handle this')

    def write(self, obj, resource, **kargs):
        path = data.path(resource)
        md = data.metadata(resource)

        #reference provider from data alias
        pd = params.metadata()['providers'][md['provider']]

        if pd['service'] == 'fs':
            if md['format']=='csv':
                return obj.write.csv(path, **kargs)
            elif md['format']=='parquet':
                return obj.write.parquet(path, **kargs)
        elif pd['service'] == 'sqlite':
            url = "jdbc:sqlite:" + pd['path']
            driver = "org.sqlite.JDBC"
            return obj.write.format('jdbc').option('url', url)\
                      .option("dbtable", md['table']).option("driver", driver).save(**kargs)
        elif pd['service'] == 'mysql':
            url = "jdbc:mysql://{}:{}/{}".format(pd['hostname'],pd.get('port', '3306'),pd['database'])
            driver = "com.mysql.jdbc.Driver"
            return obj.write.format('jdbc').option('url', url)\
                   .option("dbtable", md['table']).option("driver", driver)\
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

class NumpyEngine():
    def __init__(self, name, config):
        import numpy as np

        self._ctx = np
        self.info = {'name': name, 'context':'numpy', 'config': config}

    def context(self):
        return self._ctx

    def read(self, resource, **kargs):
        uri = data.uri(resource)
        path = data.path(resource)
        md = data.metadata(resource)

        if md['format']=='csv':
            return self._ctx.genfromtxt(path, **kargs)
        else:
            raise('downt know how to handle this')

    def write(self, obj, resource, **kargs):
        uri = data.uri(resource)
        path = data.path(resource)
        md = data.metadata(resource)

        if md['format']=='csv':
            return self._ctx.savetxt(path, obj, **kargs)
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

        if cn['context']=='numpy':
            engine = NumpyEngine(name, config)
            engines[name] = engine

    return engine
