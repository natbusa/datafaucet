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
    
    def load(self, resource):
        uri = data.geturi(resource)
        return self.ctx.read_csv(uri)
     

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

    return engine

# Examples
#    
# df = engines.get('python').load('train')
#
# engine = engines.get('spark')
# df = engine.load('train')
