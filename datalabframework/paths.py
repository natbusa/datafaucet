import os

_rootdir = os.getcwd()

def find_rootdir(filenames = ('__main__.py', 'main.ipynb')):
    path = os.getcwd()

    while os.path.isdir(path):
        ls = os.listdir(path)
        if any([f in ls for f in filenames]):
            return os.path.abspath(path)
        else:
            path += '/..'

    # nothing found: using the current working dir
    return os.getcwd()

def set_rootdir(path=None):
    global _rootdir

    try:
        path = path if os.path.isdir(path) else find_rootdir()
        _rootdir = os.path.abspath(path)
    except:
        _rootdir = os.getcwd()

    return _rootdir

def rootdir():
    return _rootdir
