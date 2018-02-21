import os
import base64
import binascii

import yaml

from . import project

from copy import deepcopy

def merge(a, b):
    if isinstance(b, dict) and isinstance(a, dict):
        a_and_b = a.keys() & b.keys()
        every_key = a.keys() | b.keys()
        return {k: merge(a[k], b[k]) if k in a_and_b else 
                   deepcopy(a[k] if k in a else b[k]) for k in every_key}
    return deepcopy(b)

_metadata = dict()

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
        filenames = [
            project.rootpath()+'/metadata.yml',
            './metadata.yml'
        ]
        
        for filename in filenames:
            f = open(filename,'r')
            params = yaml.load(f)
            metadata = merge(metadata, params)

    #return the updated arg_dict as a named tuple
    _metadata = metadata

def metadata():
    if not _metadata:
        read_metadata()
        
    return _metadata