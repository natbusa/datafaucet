from . import params
from . import project

def geturi(datasource):
    md = params.metadata()
    
    ds = md['data']['resources'].get(datasource)
    pd = md['data']['providers'][ds['provider']]
    
    if pd['service']=='fs':
    
        root = pd['rootpath']
        if not root[0]=='/':
            root = '{}/{}'.format(project.rootpath(), root)
            path = '{}/{}'.format(root, ds['path'])
        
        return path
    
    return None
   
