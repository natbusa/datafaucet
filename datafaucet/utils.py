import os
import sys

from copy import deepcopy

import traceback
from subprocess import Popen, PIPE


def print_trace(limit=None):
    stack = ([str([x[0], x[1], x[2]]) for x in traceback.extract_stack(limit=limit)])
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
                e = d.get(k[0])
                if isinstance(e, dict):
                    yield (k[0], dict(to_ordered_dict_generator(e, k[1])))
                else:
                    yield (k[0], e)
            else:
                e = d.get(k)
                yield (k, e)

    return dict(to_ordered_dict_generator(d, keys))


def merge(a, b):
    """
    Hierarchical merge of dictionaries, lists, tuples and sets.
    If b is None, it keeps a, otherwise it merges with a.
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

    # if b is None, inherit from a
    return deepcopy(a if b is None else b)


def find(filename, directory):
    for dirpath, dirnames, files in os.walk(directory):
        for name in files:
            if name == filename:
                return True
    return False


def relpath(paths, basepath):
    def r(path, basepath):
        if path and path.startswith('/') and not basepath:
            return path
        else:
            return os.path.relpath(path, basepath)

    if isinstance(paths, list):
        return [r(path, basepath) for path in paths if path]
    else:
        return r(paths, basepath)


def abspath(paths, basepath):
    if isinstance(paths, list):
        return [os.path.join(basepath, path) for path in paths if path]
    else:
        return os.path.join(basepath, paths) if paths else None


def get_home_dirname(command_abspath, subpath='bin'):
    pos = command_abspath.find(f'/{subpath}/')
    if pos == -1:
        # assume that the homedir is the dirname of the command path
        return os.path.dirname(command_abspath)
    else:
        # if the subpath is found in the command path, 
        # the home dir is anythng before the subpath
        return os.path.dirname(command_abspath[:pos + 1])


def get_tool_home(command, env_variable, subpath='bin'):
    try:
        command_abspath = os.path.join(os.environ[env_variable], subpath, command)
        os.stat(command_abspath)
        return (get_home_dirname(command_abspath, subpath), env_variable)
    except:
        pass

    try:
        output = run_command('which', command)
        command_abspath = output[0]
        return (get_home_dirname(command_abspath, subpath), 'PATH')
    except:
        return ('', '')


def run_command(*args):
    process = Popen(' '.join(args), shell='/bin/bash', stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()
    return output.decode('ascii').splitlines()


def python_version():
    return '.'.join([str(x) for x in sys.version_info[0:3]])


def str_join(lst, sep=' '):
    return sep.join([x for x in lst if x])

def flatten_dict(dictionary, accumulator=None, parent_key=None, separator="."):
    if accumulator is None:
        accumulator = {}

    for k, v in dictionary.items():
        k = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            flatten_dict(dictionary=v, accumulator=accumulator, parent_key=k)
            continue

        accumulator[k] = v

    return accumulator