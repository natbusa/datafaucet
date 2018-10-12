from . import params
from . import project

def uri(resource):
    return params.resource_unique_name(resource, project.filename(False))

def _url(md):
    pmd = md['provider']
    rmd = md['resource']

    #defaults
    if  pmd['service'] == 'local':
        pmd['path'] = pmd.get('path',project.rootpath())
        if pmd['path'][0]!='/':
            pmd['path'] = '{}/{}'.format(project.rootpath(), pmd['path'])
    else:
        pmd['path'] = pmd.get('path','')

    pmd['hostname'] = pmd.get('hostname', '127.0.0.1')
    rmd['path'] = rmd.get('path','')

    if  pmd['service'] == 'local':
        url = "file://{}/{}".format(pmd['path'], rmd['path'])
    elif pmd['service'] == 'hdfs':
        url = "hdfs://{}:{}/{}/{}".format(pmd['hostname'],pmd.get('port', '8020'),pmd['path'],rmd['path'])
    elif pmd['service'] == 'minio':
        url = "s3a://{}".format(os.path.join(pmd['path'],rmd['path']))
    elif pmd['service'] == 'sqlite':
        url = "jdbc:sqlite:" + pmd['path']
    elif pmd['service'] == 'mysql':
        url = "jdbc:mysql://{}:{}/{}".format(pmd['hostname'],pmd.get('port', '3306'),pmd['database'])
    elif pmd['service'] == 'postgres':
        url = "jdbc:postgresql://{}:{}/{}".format(pmd['hostname'],pmd.get('port', '5432'),pmd['database'])
    elif pmd['service'] == 'mssql':
        url = "jdbc:sqlserver://{}:{};databaseName={}".format(pmd['hostname'],pmd.get('port', '1433'),pmd['database'])
    else:
        url = None

def metadata(resource=None, path=None, provider=None):
    md = params.metadata()

    ds = md['resources'].get(uri(resource))
    pd = md['providers'].get(provider)

    if ds and ds['provider'] in md['providers']:
        pd = md['providers'][ds['provider']]
        d = {'resource':ds, 'provider':pd}

    elif pd and path:
        d = {
            'provider': pd,
            'resource':{
                'path':path,
                'provider':provider
                }
            }
    else:
        print('Resource not found: must specify either path and provider alias, or the resource alias')
        return None

    d['url'] = _url(d)
    return d
