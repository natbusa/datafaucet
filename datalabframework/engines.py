from . import params
from . import data

# purpose of engines
# abstract engine init, data read and data write
# and move this information to metadata

# it does not make the code fully engine agnostic though.

engines = dict()

class SparkEngine():
    def __init__(self, name, config):
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SQLContext

        conf = SparkConf()
        if 'jobname' in config:
            conf.setAppName(config.get('jobname'))

        conf.setMaster(config.get('master', 'local[4]'))
        self._ctx = SQLContext(SparkContext(conf=conf))
        self.info = {'name': name, 'context':'spark', 'config': config}

    def context(self):
        return self._ctx

    def read(self, resource, **kargs):
        path = data.path(resource)
        md = data.metadata(resource)

        if md['format']=='csv':
            return self._ctx.read.csv(path, **kargs)
        elif md['format']=='parquet':
            return self._ctx.read.parquet(path, **kargs)
        else:
            raise('downt know how to handle this')

    def write(self, obj, resource, **kargs):
        path = data.path(resource)
        md = data.metadata(resource)

        if md['format']=='csv':
            return obj.write.csv(path, **kargs)
        elif md['format']=='parquet':
            return obj.write.parquet(path, **kargs)
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
            return obj.tp_parquet(path, **kargs)
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
