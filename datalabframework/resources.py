import os
import re
import ctypes
import functools

from urllib.parse import urlparse
from collections import namedtuple
from copy import deepcopy

from datalabframework import metadata
from datalabframework.paths import rootdir
from datalabframework._utils import merge, to_ordered_dict
from datalabframework.yaml import YamlDict

import datalabframework.logging as log

Urn = namedtuple('Urn', ['scheme', 'user', 'password', 'host', 'port', 'path', 'params', 'query', 'fragment'])

def filter_empty(lst):
    return [x for x in lst if x != '' and x is not None]

def tsplit(s, sep, shift='left'):
    s = s.split(sep)
    if shift=='left':
        return (s[0],s[1]) if len(s)>1 else ('', s[0])
    else: # right
        return (s[0],s[1]) if len(s)>1 else (s[0], '')
    
def urnparse(s):
    scheme, url = tsplit(s, '//')

    path_only = ((not scheme) or 
        scheme.startswith('jdbc:sqlite') or
        scheme.startswith('s3a'))        
        
    url = urlparse(url) if path_only else urlparse('scheme://'+url)

    path = url.path
    query = url.query
    
    scheme = filter_empty(scheme.split(':'))
    
    auth, netloc = tsplit(url.netloc, '@')
    user, password = tsplit(auth, ':', 'right')
    host, port = tsplit(netloc,':', 'right') 
    
    # parsing oracle thin urn for user, password
    oracle_thin_scheme = len(scheme)==4 and ':'.join(scheme[0:3])=='jdbc:oracle:thin'
    if oracle_thin_scheme and scheme[-1][-1]=='@':
        o_user, o_password = tsplit(scheme[-1].rstrip('@'), '/', 'right')
        user = o_user or user
        password = o_password or password
    
    # parsing oracle params
    if oracle_thin_scheme:
        path, *params = path.split(',')
        query = query + '&'.join(filter_empty(params))
        
    # parsing mssql params
    jdbc_mssql_scheme = len(scheme)==2 and ':'.join(scheme[0:2])=='jdbc:sqlserver'
    if jdbc_mssql_scheme:
        netloc, *params = netloc.split(';')
        host, port = tsplit(netloc,':', 'right') 
        query = query + '&'.join(filter_empty(params))
    
    params = filter_empty(query.split('&'))
    params = [tuple(p.split('=')) for p in params]
    
    urn = Urn(scheme,user, password, host, port, path, params, query, url.fragment)
    return urn 

def path_to_jdbc(md):
    
    database = md['database']
    table = md['table']
    path = md['path'] or ''
    
    e = filter_empty(path.split('/'))
    
    if len(e)==0:
        pass;
    elif len(e)==1:
        if database:
            table = e[0] or None
        else:
            database = e[0] or None
    else:    
        database = e[0] or None
        table = e[1] or None
    return database, table

def get_default_md():
    f = [
        'service',
        'format',
        
        'host',
        'port',

        'driver', 
        'database', 
        'schema', 
        'table', 
        
        'user',
        'password',
        
        'path', 
        'options',
        
        'provider'
    ]

    return dict(zip(f, [None for _ in range(len(f))]))

def metadata_overrides(md, host=None, service=None, port=None, user=None, password=None,
                driver=None, database=None, schema=None, table=None, format=None, 
                hostname=None, username=None, **options):
    
    md['host'] = host or hostname or md['host'] or md.get('hostname')
    md['port'] = port or md['port']

    md['service'] = service or md['service']
    md['format'] = format or md['format']

    md['user'] =  user or username or md['user'] or md.get('username')
    md['password'] =  password or md['password']
    
    md['database'] =  database or md['database']
    md['schema'] =  schema or md['schema']
    md['table'] = table or md['table']
    md['driver'] =  driver or md['driver']
    md['options'] = options or md['options']
    
    if database and table:
        md['path'] = f'{database}/{table}'
    elif database:
        md['path'] = database
    elif table:
        md['path'] = table

    return md

def resource_from_urn(urn):
    
    if urn.scheme and urn.scheme[0]=='jdbc':
        service, format = urn.scheme[1], urn.scheme[0]        
    else:
        _, format = os.path.splitext(urn.path)
        format = format[1:] if len(format)>1 else ''
        service = urn.scheme[0] if urn.scheme else ''
    
    md = get_default_md()
    
    md['service'] = service
    md['format'] = format
    md['host'] = urn.host
    md['port'] = urn.port

    md['path'] = urn.path

    md['user'] = urn.user
    md['password'] = urn.password

    md['options'] = dict(urn.params)
    
    for k,v in md.items():
        if not v:
            md[k] = None
    
    return md

def to_resource(url_alias=None, *args, **kwargs):
    
    md = None
    
    # if a dict, create from dictionary
    if isinstance(url_alias, dict):
        md = metadata_overrides(get_default_md(), **url_alias)
    
    # if a string, and a metadata profile is loaded, check for aliases
    if metadata.profile:
        if not md and url_alias in metadata.profile.get('resources', {}).keys():
            md = metadata.profile['resources'][url_alias]
        
        if not md and url_alias in metadata.profile.get('providers', {}).keys():
            md = metadata.profile['providers'][url_alias]
    
    # if nothing found yet, interpret as a urn/path
    if not md and url_alias:
        md = resource_from_urn(urnparse(url_alias))
        
    # empty default
    if not md:
        md = get_default_md()

    # sanitize path if it's a url
    if md['path']:
        url_md = resource_from_urn(urnparse(md['path']))
        md = merge(url_md, md)
        md['path'] = url_md['path']

    # override using kwargs
    md = metadata_overrides(md, **kwargs)

    if 'hostname' in md:
        del md['hostname']

    if 'username' in md:
        del md['username']
    
    return md

def get_format(md):
    
    if md['format']:
        return md['format']
    
    # get the provider format
    if md['service'] in ['sqlite', 'mysql', 'postgres', 'mssql', 'oracle']:
        return 'jdbc'

    if md['service'] in ['elastic']:
        return 'json'

    # extract the format from file extension
    _, ext = os.path.splitext(md['path'])

    # default is parquet
    return ext or None

def get_driver(service):
    drivers = {
        'sqlite': 'org.sqlite.JDBC',
        'mysql': 'com.mysql.cj.jdbc.Driver',
        'postgres': 'org.postgresql.Driver',
        'mssql': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'oracle': 'oracle.jdbc.driver.OracleDriver'
    }
    return drivers.get(service)

def get_port(service):
    ports = {
        'hdfs': 8020,
        'mysql': 3306,
        'postgres': 5432,
        'mssql': 1433,
        'oracle': 1521,
        'elastic': 9200,
        's3a':9000
    }
    return ports.get(service)

def get_url(md):
    service = md['service']
    path = md['path']
    
    if  service in ['local', 'file']:
        url = path
    elif service == 'sqlite':
        url = f"jdbc:sqlite:{path}"
    elif service == 'hdfs':
        url = f"hdfs://{md['host']}:{md['port']}{md['path']}"
    elif service in ['minio', 's3a']:
        url = f"s3a://{md['path']}'"
    elif service == 'mysql':
        url = f"jdbc:mysql://{md['host']}:{md['port']}/{md['database']}"
    elif service == 'postgres':
        url = f"jdbc:postgresql://{md['host']}:{md['port']}/{md['database']}"
    elif service == 'mssql':
        url = f"jdbc:sqlserver://{md['host']}:{md['port']};databaseName={md['database']}"
    elif service == 'oracle':
        url = f"jdbc:oracle:thin:@//{md['host']}:{md['port']}/{md['database']}"
    elif service == 'elastic':
        url = f"http://{md['host']}:{md['port']}/{md['database']}"

    return url


def process_metadata(md):
    
    # update format from 
    md['format'] = get_format(md)
    
    # if no service, at this point use file
    md['service'] = md['service'] or 'file'
    
    # standardize some service names
    services = {
        'minio': 's3a',
        'local': 'file'
    }
    md['service'] = services.get(md['service'], md['service'])
    
    # if no host, use localhost
    md['host'] = md['host'] or '127.0.0.1'
        
    # if local file system and rel path, prepend rootdir
    if md['service'] in ['file', 'sqlite'] and not os.path.isabs(md['path']):
        md['path'] = os.path.join(rootdir(), md['path'])

    # if service is s3a, remove leading '/'
    if md['service'] == 's3a' and md['path']:
        md['path'] = md['path'].lstrip('/')

    # generate database, table from path
    if md['format']=='jdbc':
        md['database'], md['table']  = path_to_jdbc(md)
        md['path'] = None

        # set driver
        md['driver'] = md['driver'] or get_driver(md['service'])
        
        # if not table, provide no result query
        md['table'] = md['table'] or 'SELECT 0 as result where 1 = 0'
        
        # if schema is not yet defined, 
        # take the default for each service
        default_schemas = {
            'mysql': md['database'],
            'mssql': 'dbo',
            'postgres': 'public',
            'oracle': md['user']
        }
        
        md['schema'] = md['schema'] or default_schemas.get(md['service'])
        
        # if SQL query is detected, 
        # wrap the resource path as a temp table
        sql_query = md['table']
        sql_query = sql_query.replace('\n', ' ')
        sql_query = sql_query.replace('\t', ' ')
        sql_query = sql_query.replace('\r', ' ')
        sql_query = ' '.join(sql_query.split())
        sql_query = sql_query.rstrip(' ')
        sql_query = sql_query.rstrip(';')

        if any([x in sql_query.lower() for x in [' from ', ' where ']]):
            md['table'] = '( {} ) as _query'.format(sql_query)

    md['port'] = md['port'] or get_port(md['service'])
    md['url'] = get_url(md)

    md['options'] = md['options'] or {}
    
    h_list = [hash(md[k]) for k in ['url', 'format', 'table', 'database']]
    md['hash'] = functools.reduce(lambda a,b : a^b, h_list)
    md['hash'] = hex(ctypes.c_size_t(md['hash']).value)
    
    return md

def assemble_metadata(md):
    keys = [
        'hash',
        'url',
        'service',
        'format',
        
        'host'
    ]
    
    if md['service'] != 'file':
        keys.append('port')

    if md['service'] == 's3a' or md['format'] == 'jdbc':
        keys.extend([
            'user',
            'password'])
        
    if md['format'] == 'jdbc':
        keys.extend([
            'driver',
            'database',
            'schema',
            'table'])
        
    keys.append('options')
    return YamlDict(to_ordered_dict(md, keys))
        
def resource(path_or_alias_or_url=None, provider_path_or_alias_or_url=None, 
        host=None, service=None, port=None, user=None, password=None,
        driver=None, database=None, schema=None, table=None, format=None, 
        hostname=None, username=None, **options):

    prov = provider_path_or_alias_or_url
    path = path_or_alias_or_url
    
    prov_is_dict = isinstance(prov, dict)
    path_is_dict = isinstance(path, dict)
    
    # get the resource, by alias metadata or by url
    rmd = to_resource(path, host=host, service=service, port=port, 
        user=user, password=password, driver=driver, database=database, 
        schema=schema, table=table, format=format, hostname=hostname, 
        username=username, **options)
    
    # get the provider by reference from the resource, if available
    prov = prov or rmd.get('provider')
    
    # get the provider, by alias metadata or by url
    pmd = to_resource(prov)


    # merge provider and resource metadata
    md = merge(pmd,rmd)
        
    # concatenate paths
    md['path'] = os.path.join(pmd['path'] or '', rmd['path'] or '')

    #process metadata
    md = process_metadata(md)
    
    # assemble output
    md = assemble_metadata(md)

    return md