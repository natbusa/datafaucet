from . import params
from . import project

def uri(resource):
    return params.resource_unique_name(resource, project.filename(False))

def metadata(resource):
    md = params.metadata()
    ds = md['resources'].get(uri(resource))
    ds['provider'] = md['providers'][ds['provider']]
    
    return ds