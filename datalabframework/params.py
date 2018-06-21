import os
import base64
import binascii

import yaml

from . import project
from . import notebook

from copy import deepcopy

def merge(a, b):
    if not a:
        a = dict()

    if not b:
        b = dict()

    if isinstance(b, dict) and isinstance(a, dict):
        a_and_b = a.keys() & b.keys()
        every_key = a.keys() | b.keys()
        return {k: merge(a[k], b[k]) if k in a_and_b else
                   deepcopy(a[k] if k in a else b[k]) for k in every_key}

    return deepcopy(b)

_metadata = dict()

def get_metadata_files():

    top  = project.rootpath()
    exclude = ['metadata.ignore.yml']

    lst = list()
    for root, dirs, files in os.walk(top, topdown=True):
        dirs[:] = [d for d in dirs if d not in exclude]
        for file in files:
            if file=='metadata.yml':
                basedir = root[len(path):].lstrip('/')
                filename = os.path.join(basedir, file)
                lst.append(filename)
    return lst

def read_metadata(envvar='DLF_METADATA', encode='utf-8'):
    global _metadata

    v = os.getenv(envvar)
    metadata = dict()
    if v:
        try:
            md = envvar
            if encode=='base64':
                md = base64.b64decode(v)
                md = pdata.decode('utf-8')

            params = yaml.load(md)
            metadata = merge(metadata, params)
        except:
            pass

        try:
            f = open(v,'r')
            params = yaml.load(f)
            metadata = merge(metadata, params)
        except:
            pass

    else:
        l = get_metadata_files()
        filenames = ['{}/{}'.format(project.rootpath(),name) for name in l]

        for filename in filenames:
            f = open(filename,'r')
            params = yaml.load(f)

            path = notebook.filename(filename, '')[0].replace('/','.')
            prefix = '.' if path else ''
            try:
                d = params['data']['resources']
                r = dict()
                for k,v in d.items():
                    if k.startswith('.'):
                        r[k] = v
                    else:
                        alias_abs = '{}{}.{}'.format(prefix, path,k)
                        r[alias_abs] = v

                params['data']['resources'] = r
            except:
                pass

            metadata = merge(metadata, params)

    #return the updated arg_dict as a named tuple
    _metadata = metadata

def metadata():
    read_metadata()
    return _metadata
