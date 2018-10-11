from . import params
from . import project

def uri(resource):
    return params.resource_unique_name(resource, project.filename(False))

def metadata(resource=None, path=None, provider=None):
    md = params.metadata()
    ds = md['resources'].get(uri(resource), None)
    if ds and ds['provider'] in md['providers']:
        pd = md['providers'][ds['provider']]
        pd['alias'] = ds['provider']
        return {'resource':ds, 'provider':pd}
    else:
        pd = params.metadata()['providers'].get(provider, None)
        if pd and path:
            pd['alias'] = provider
            ds = {'provider': pd, 'resource':{'path':path}}
            return ds
        else:
            print('Resource not found: must specify either path and provider alias, or the resource alias')
            return None
