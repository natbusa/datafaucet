from . import params
from . import project
from . import notebook
from . import utils

def uri(datasource):
    if not datasource.startswith('.'):
        f = notebook.get_filename()
        p = f.rfind('/')
        resource_path = f[:p+1].replace('/','.') if p>0 else '.'
        datasource = '{}{}'.format(resource_path, datasource)
    
    return datasource

def metadata(datasource):
    md = params.metadata()
    ds = md['resources'].get(uri(datasource))

    return ds

def path(datasource):
    md = params.metadata()
    ds = metadata(datasource)
    pd = md['providers'][ds['provider']]

    if pd['service']=='fs':

        root = pd['rootpath']
        if not root[0]=='/':
            root = '{}/{}'.format(project.rootpath(), root)
            path = '{}/{}'.format(root, ds['path'])

        return path

    return None
