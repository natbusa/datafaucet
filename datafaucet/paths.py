import os

_rootdir = os.getcwd()


def find_rootdir(filenames=('__main__.py', 'main.ipynb')):
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

    if path is None:
        _rootdir = find_rootdir()

    elif path and os.path.isdir(path):
        _rootdir = os.path.abspath(path)
    else:
        # do nothing
        pass


def rootdir():
    return _rootdir
