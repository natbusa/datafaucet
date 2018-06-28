from . import params
from . import project
from . import notebook
from . import utils

def uri(resource):
    return resource_unique_name(resource, notebook.get_filename(False))

def metadata(resource):
    md = params.metadata()
    ds = md['resources'].get(uri(resource))

    return ds

def path(resource):
    md = params.metadata()
    ds = metadata(resource)
    pd = md['providers'][ds['provider']]

    if pd['service']=='fs':

        root = pd['rootpath']
        if not root[0]=='/':
            root = '{}/{}'.format(project.rootpath(), root)
            path = '{}/{}'.format(root, ds['path'])

        return path

    return None
