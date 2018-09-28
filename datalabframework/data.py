from . import params
from . import project

def uri(resource):
    return params.resource_unique_name(resource, project.notebook(False))

def metadata(resource, provider=None):
    md = params.metadata()
    ds = md['resources'].get(uri(resource), None)
    if ds and ds['provider'] in md['providers']:
        ds['provider'] = md['providers'][ds['provider']]
        return ds
    else:
        pd = params.metadata()['providers'].get(provider)
        if pd:
            ds = {'path': resource, 'provider': pd}
            return ds
        else:
            print('no valid resource found')
            return None
