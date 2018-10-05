from . import params
from . import project

def uri(resource):
    return params.resource_unique_name(resource, project.filename(False))

def metadata(resource=None, path=None, provider=None):
    md = params.metadata()
    ds = md['resources'].get(uri(resource), None)
    if ds and ds['provider'] in md['providers']:
        ds['provider'] = md['providers'][ds['provider']]
        return ds
    else:
        pd = params.metadata()['providers'].get(provider)
        if pd and path:
            ds = {'path': path, 'provider': pd}
            return ds
        else:
            print('Resource not found: must specify either path and provider alias, or the resource alias')
            return None
