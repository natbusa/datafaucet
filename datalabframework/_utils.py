import os
import git

from copy import deepcopy
from collections import Mapping
from datalabframework.yaml import yaml

import traceback

def print_trace(limit=None): 
    stack =([str([x[0], x[1], x[2]]) for x in traceback.extract_stack(limit=limit)])
    print('trace')
    print('   \n'.join(stack))

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

def to_ordered_dict(d, keys):
    def to_ordered_dict_generator(d, keys):
        for k in keys:
            if isinstance(k, tuple):
                e = d.get(k[0], {})
                yield (k[0], dict(to_ordered_dict_generator(e, k[1])))
            else:
                e = d.get(k)
                yield (k, e)

    return dict(to_ordered_dict_generator(d, keys))

class YamlDict(Mapping):
    """
    A helper class which provides read-only access to the dictionary and
    representation in yaml formal, for enhanced readability
    the to_dict() method return the underlying dictionary
    """
    def __init__(self, m=None, **kwargs):
        if isinstance(m,str):
            self._data = yaml.load(m)
        else:
            self._data = dict(m,**kwargs) if m is not None else dict(**kwargs)
            
    def __getitem__(self, key):
        return self._data[key]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        return yaml.dump(self._data)

    def __dict__(self):
        return self._data
    
    def to_dict(self):
        return self._data

def merge(a, b):
    """
    Hierarchical merge of dictionaries, lists, tuples and sets.
    If b is falsy, it keeps a, otherwise it merges with a.
    In case of ambiguities, b overrides a
    it returns is a deepcopy, not a reference of the original objects.
    """

    if isinstance(b, dict) and isinstance(a, dict):
        a_and_b = set(a.keys()) & set(b.keys())
        every_key = set(a.keys()) | set(b.keys())
        return {k: merge(a[k], b[k]) if k in a_and_b else deepcopy(a[k] if k in a else b[k]) for k in every_key}
    
    if isinstance(b, list) and isinstance(a, list):
        return deepcopy(a) + deepcopy(b)

    if isinstance(b, tuple) and isinstance(a, tuple):
        return deepcopy(a) + deepcopy(b)

    if isinstance(b, set) and isinstance(a, set):
        return deepcopy(a) | deepcopy(b)

    #if b is None, inherit from a
    return deepcopy(b if b else a)


def repo_data(rootdir=None, search_parent_directories=True):
    """
    :param rootdir: the root directory where to look for the repo. (default is current working dir)
    :param search_parent_directories: repo search upwards for a valid .git directory object
    :return: a dictionary with git repository info, if available
    """

    if rootdir is None:
        rootdir = os.getcwd()

    msg = {
            'type': None,
            'committer': '',
            'hash': 0,
            'commit': 0,
            'branch': '',
            # How to get url
            'url': '',
            'name': '',
            # How to get humanable time
            'date': '',
            'clean': False
        }

    try:
        repo = git.Repo(rootdir, search_parent_directories=search_parent_directories)
        (commit, branch) = repo.head.object.name_rev.split(' ')
        msg['type'] = 'git'
        msg['committer'] = repo.head.object.committer.name
        msg['hash'] = commit[:7]
        msg['commit'] = commit
        msg['branch'] = branch
        msg['url'] = repo.remotes.origin.url
        msg['name'] = repo.remotes.origin.url.split('/')[-1]
        msg['date'] = repo.head.object.committed_datetime.isoformat()
        msg['clean'] = len(repo.index.diff(None)) == 0
    except:
        pass

    return msg
