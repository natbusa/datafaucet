import os
import ctypes
from urllib.parse import urlparse

from datalabframework.yaml import YamlDict
from datalabframework._utils import merge, to_ordered_dict

from urllib.parse import urlparse
from collections import namedtuple

Urn = namedtuple('Urn', ['scheme', 'user', 'password', 'host', 'port', 'path', 'params', 'query', 'fragment'])

def tsplit(s, sep, shift='left'):
    s = s.split(sep)
    if shift=='left':
        return (s[0],s[1]) if len(s)>1 else ('', s[0])
    else: # right
        return (s[0],s[1]) if len(s)>1 else (s[0], '')
    
def urnparse(s):
    scheme, url = tsplit(s, '//')
    
    url = urlparse('scheme://'+url) if scheme else urlparse(url)

    scheme = scheme or 'file:'
    scheme = scheme.split(':')
    
    auth, netloc = tsplit(url.netloc, '@')
    user, password = tsplit(auth, ':', 'right')
    host, port = tsplit(netloc,':', 'right') 

    # parsing oracle thin urn for user, password
    if scheme[-1] and scheme[-1][-1]=='@':
        o_user, o_password = tsplit(scheme[-1].rstrip('@'), '/', 'right')
        user = o_user or user
        password = o_password or user
        
    #print(url)
    urn = Urn(scheme,user, password, host, port, url.path, url.params, url.query, url.fragment)
    return urn 

def _url(d):

    url = d.get('url')
    
    if url:
        return url
    
    service = d['service']

        fullpath = os.path.join(d['provider_path'],d['resource_path'])

        if  service in ['local', 'file']:
            url = os.path.realpath(fullpath)
        elif service == 'sqlite':
            url = 'jdbc:sqlite:{}'.format(os.path.realpath(fullpath))
        elif service == 'hdfs':
            url = 'hdfs://{}:{}{}'.format(d['host'], d['port'], fullpath)
        elif service in ['minio', 's3a']:
            url = 's3a://{}/{}'.format(d['provider_path'],d['resource_path'])
        elif service == 'mysql':
            url = 'jdbc:mysql://{}:{}/{}'.format(d['host'],d['port'], d['database'])
        elif service == 'postgres':
            url = 'jdbc:postgresql://{}:{}/{}'.format(d['host'], d['port'], d['database'])
        elif service == 'mssql':
            url = 'jdbc:sqlserver://{}:{};databaseName={}'.format(d['host'], d['port'], d['database'])
        elif service == 'oracle':
            url = 'jdbc:oracle:thin:@//{}:{}/{}'.format(d['host'], d['port'], d['database'])
        elif service == 'elastic':
            url = 'http://{}:{}/{}'.format(d['host'], d['port'], d['database'])

    return url

def _port(service_name):
    ports = {
        'hdfs': 8020,
        'mysql': 3306,
        'postgres': 5432,
        'mssql': 1433,
        'oracle': 1521,
        'elastic': 9200,
        'minio':9000
    }
    return ports.get(service_name)

def _format(d):

    # get the provider format
    if d.get('service') in ['sqlite', 'mysql', 'postgres', 'mssql', 'oracle']:
        return 'jdbc'

    if d.get('service') in ['elastic', 'mongodb']:
        return 'nosql'

    formats = [
        'csv', 
        'json', 
        'jsonl',
        'parquet'
    ]
    
    path = d.get('resource_path', '')
    path = path.split('.') if path else ''
    if len(path)>1 and path[-1] in formats:
        return path[-1]

    # default is parquet
    return d.get('format', 'parquet')

def _driver(d):
    drivers = {
        'sqlite': 'org.sqlite.JDBC',
        'mysql': 'com.mysql.cj.jdbc.Driver',
        'postgres': 'org.postgresql.Driver',
        'mssql': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'oracle': 'oracle.jdbc.driver.OracleDriver'
    }
    return drivers.get(d.get('service'))


def _get_resource_metadata(metadata=dict(), resource=None, provider=None):

    if 'resources' not in metadata.keys():
        metadata['resources'] = {}

    #first match by resource alias
    rmd = metadata['resources'].get(resource, {})
    if rmd:
        rmd['alias'] = resource
        if provider:
            rmd['provider'] = provider
        if not rmd.get('path'):
            rmd['path'] = resource

    # match resource not as alias but as a path in any of the available resources,
    # using the given provider name and provider name
    if not rmd:
        for resource_alias in metadata['resources'].keys():
            resource_candidate = metadata['resources'][resource_alias]
            if  resource_candidate.get('path') and resource_candidate.get('path') == resource and \
                    resource_candidate.get('provider') and resource_candidate.get('provider') == provider:
                rmd = resource_candidate
                rmd['alias'] = resource_alias
                break

    # if nothing yet, try with path alone
    if not rmd:
        for resource_alias in metadata['resources'].keys():
            resource_candidate = metadata['resources'][resource_alias]
            if resource_candidate.get('path') and resource_candidate.get('path') == resource:
                rmd = resource_candidate
                rmd['alias'] = resource_alias
                if provider:
                    rmd['provider'] = provider
                break

    #still nothing use path and provider as minimal resource info
    if not rmd:
        if provider:
            rmd['provider'] = provider

        # if resource is given use it as a path
        if resource:
            rmd['path'] = resource

    # nothing found, return None
    return rmd

def _get_provider_metadata(metadata=dict(), rmd=None):
    #if no resource dictionary, return an empty dictionary
    if not rmd:
        return {}

    provider = rmd.get('provider')
    providers = metadata.get('providers', {})

    pmd = {}
    if provider:
        if provider in providers.keys():
            pmd = providers.get(provider, {})
            pmd['alias'] = provider
        else:
            # if no valid provider alias at this point, use provider as a path
            pmd = {'path':provider, 'alias': None}

    return pmd

def _override_metadata(access, param, pmd=dict(), rmd=dict()):
    d = merge(pmd.get(access, {}).get(param, {}), rmd.get(access, {}).get(param, {}))
    return d

def _build_resource_metadata(rootdir, pmd={}, rmd={}, user_md=None):

    if user_md is None:
        user_md = dict()
        
    d = merge(pmd, rmd)
    
    d['provider_alias'] = pmd.get('alias', pmd.get('path', ''))
    d['resource_alias'] = rmd.get('alias', rmd.get('path', ''))
    
    d['provider_path']  = pmd.get('path', pmd.get('alias', ''))
    d['resource_path']  = rmd.get('path', rmd.get('alias', ''))

    # cleanup aliases
    d['provider_alias'] = os.path.splitext(os.path.split(d['provider_alias'])[1])[0]
    d['resource_alias'] = os.path.splitext(os.path.split(d['resource_alias'])[1])[0]

    d.pop('alias', None)
    d.pop('path', None)
    
    d['rootdir'] = rootdir

    urn = urlparse(d['resource_path'])
    
    d['service'] = urn.scheme or d.get('service', 'file')
    d['resource_path'] = urn.path

    #default hostname is localhost
    d['host'] = urn.netloc or d.get('hostname', d.get('host','127.0.0.1'))

    # if service is file or sqlite,
    # relative provider path is allowed, and prefixed with rootpath
    if d['service'] in ['file', 'sqlite'] and not os.path.isabs(d['provider_path']):
        d['provider_path'] = os.path.realpath(
            os.path.join(d['rootdir'], d['provider_path']))
    
    if d['service'] in ['file', 'sqlite'] :
        (path, filename) = os.path.split(d['resource_path'])
        if path and path.startswith(d['rootdir']):
            d['resource_path'] = os.path.join(os.path.relpath(path, d['rootdir']),filename)
            d['provider_path'] = d['rootdir']
            d['provider_alias'] = 'file_project_path'
        else:
            d['resource_path'] = filename
            d['provider_alias'] = 'file_local_path'
            d['provider_path'] = path
    
    if d['service'] == 's3a' :
        d['resource_path'] = os.path.split(d['resource_path'])[1]
        d['provider_path'] = d['host']        
        d['provider_alias'] = 's3a_'+ d['provider_path']
        d['host'] = ''
    
    d['format'] = _format(d)
    d['driver'] = _driver(d)

    # provider path can be use as database name, if database is undefined
    # for some special rdbms, database and path are both required
    # if both database and path are provided, path is interpreted as a database schema
    # if only path or database is provided, assume that the schema is 'public'
    
    if d['format'] == 'jdbc':
        d['table'] = d['resource_path']
        d['table'] = d['table'] if d['table'] else 'SELECT 0 as result where 1 = 0'
        
        if d.get('database'):
            d['database'] = d.get('database')
            d['schema'] = d['provider_path'] if d['provider_path'] else ''
        else:
            d['database'] = d['provider_path']
            d['schema'] = ''
        
        #if schema is not yet defined, take the default for each service
        if not d['schema']:
            if  d.get('service') == 'mysql':
                d['schema'] = d['database']
            elif d.get('service') == 'mssql':
                d['schema'] = 'dbo'
            elif d.get('service') == 'postgres':
                d['schema'] = 'public'
            elif d.get('service') == 'oracle':
                d['schema'] = d.get('username', '')
            else:
                #use postgres default if service unkown
                d['schema'] = 'public'        
        
        # if format is jdbc and an SQL query is detected, 
        # wrap the resource path as a temp table
        sql_query = d['table']
        sql_query = sql_query.replace('\n', ' ')
        sql_query = sql_query.replace('\t', ' ')
        sql_query = sql_query.replace('\r', ' ')
        sql_query = ' '.join(sql_query.split())
        sql_query = sql_query.rstrip(' ')
        sql_query = sql_query.rstrip(';')

        if ' from ' in sql_query.lower():
            d['table'] = '( {} ) as _query'.format(sql_query)
    
    d['port'] = d.get('port', _port(d['service']))
    d['url'] = _url(d)

    d['options'] = d['options'] if d.get('options') else {}
    d['mapping'] = d['mapping'] if d.get('mapping') else {}
    
    # override with function provided metadata
    d = merge(d, user_md)
    
    d['hash'] = hash(d['url']) ^ hash(d['format'])
    d['hash'] = hex(ctypes.c_size_t(d['hash']).value)

    return d

resource_keys = (
        'hash',
        'url',
        'service',
        'format',
        
        'host',
        'port',

        'driver',
        'database',
        'schema',
        'table',
        'username',
        'password',
        
        'resource_path',
        'provider_path',
        
        'provider_alias',
        'resource_alias',
        
        'cache',
        'date_column',  
        'date_start',
        'date_end',
        'date_window',
        'date_partition',
        'update_column',
        'hash_column',
        'state_column',

        'options',
        'mapping',
    )

def metadata(
    rootdir=None, 
    metadata=dict(), 
    path=None, 
    provider=None, 
    md=None, 
    username=None, 
    password=None, 
    **kargs):
    """
    :param metadata: resources metadata
    :param rootdir:  directory path for relative local files
    :param path: name of the resource alias or resource path
    :param provider:  name of the provider alias
    :param md: dictionary of metadata, overrides provider and resource metadata
    :return: None or a dictionary with the resource propertiees:
    """
    rootdir = rootdir if rootdir else os.getcwd()

    rmd = _get_resource_metadata(metadata, path, provider)
    pmd = _get_provider_metadata(metadata, rmd)
    
    d = _build_resource_metadata(rootdir, pmd, rmd, md)
    
    d = merge(d, {'options': kargs})
    d = merge(d, {'username': username, 'password':password})

    return YamlDict(to_ordered_dict(d, resource_keys))
