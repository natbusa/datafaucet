from . import params
from . import data

# purpose of engines
# abstract engine init, data read and data write
# and move this information to metadata

# it does not make the code fully engine agnostic though.

engines = dict()

class PandasEngine():
    def __init__(self, name):
        import pandas as pd
        
        self.name = name
        self.ctx = pd
    
    def context(self):
        return self.ctx
    
    def read(self, resource):
        uri = data.uri(resource)
        path = data.path(resource)
        md = data.metadata(resource)
        
        if md['format']=='csv':
            return self.ctx.read_csv(path)
        if md['format']=='hdf':
            return self.ctx.read_hdf(path, uri.replace('.','_'))     
        
    def write(self, obj, resource):
        uri = data.uri(resource)
        path = data.path(resource)
        md = data.metadata(resource)
        
        if md['format']=='csv':
            return obj.to_csv(path)
        if md['format']=='hdf':
            return obj.to_hdf(path, uri.replace('.','_'))

def get(name):
    global engines

    #get
    engine = engines.get(name)
    
    if not engine:
        #create
        md = params.metadata()
        cn = md['execution']['engines'].get(name)

        if cn['kernel']=='python' and cn['loader']=='pandas':
            engine = PandasEngine(name)
            engines[name] = engine

        if cn['kernel']=='python' and cn['loader']=='numpy':
            engine = NumpyEngine(name)
            engines[name] = engine

    return engine

# Examples
#    
# df = engines.get('python').load('train')
#
# engine = engines.get('spark')
# df = engine.load('train')
