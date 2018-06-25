import os
from copy import deepcopy

from .project import rootpath

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

def lrchop(s, b='', e=''):
    if s.startswith(b) and len(b)>0:
        s = s[len(b):]
    if s.endswith(e) and len(e)>0:
        s = s[:-len(e)]
    return s

def relative_filename(s):
    return lrchop(s,rootpath()).lstrip('/')

def absolute_filename(s):
    return s if s.beginswith('/') else '{}/{}'.format(rootpath(),s)

def get_project_files(ext, exclude_dirs=[], ignore_dir_with_file='', relative_path=True):
    top  = rootpath()

    exclude = ['metadata.ignore.yml']
    metadata_filename = 'metadata.yml'

    lst = list()
    for root, dirs, files in os.walk(top, topdown=True):
        for d in exclude_dirs:
            if d in dirs:
                dirs.remove(d)

        if ignore_dir_with_file in files:
            dirs[:] = []
            next
        
        for file in files:
            if file.endswith(ext):
                f = os.path.join(root, file)
                lst.append(relative_filename(f) if relative_path else f)
    
    return lst

#get_project_files(ext='metadata.yml', ignore_dir_with_file='metadata.ignore.yml', relative_path=False)
#get_project_files(ext='.ipynb', exclude_dirs=['.ipynb_checkpoints'])