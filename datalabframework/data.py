from . import params
from . import project
from . import notebook

def uri(datasource):
    if not datasource.startswith('.'):
        path = notebook.filename()[0].replace('/','.')
        prefix = '.' if path else ''
        datasource = '{}{}.{}'.format(prefix, path,datasource)
    
    return datasource

def metadata(datasource):
    md = params.metadata()
    ds = md['data']['resources'].get(uri(datasource))

    return ds

def path(datasource):
    md = params.metadata()
    ds = metadata(datasource)
    pd = md['data']['providers'][ds['provider']]
    
    if pd['service']=='fs':
    
        root = pd['rootpath']
        if not root[0]=='/':
            root = '{}/{}'.format(project.rootpath(), root)
            path = '{}/{}'.format(root, ds['path'])
        
        return path
    
    return None
