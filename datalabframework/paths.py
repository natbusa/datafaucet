import os

_rootdir = os.getcwd()

def find_rootdir(wd):
    assert(os.path.exists(wd))

    pattern_filenames = ('__main__.py', 'main.ipynb')
    for filename in pattern_filenames:
        path = os.path.abspath(wd)
        while True:
            try:
                ls = os.listdir(path)
                if filename in ls:
                    return os.path.abspath(path)
                else:
                    path += '/..'
            except:
                break

    # nothing found: using the current working dir
    return os.path.abspath(wd)

def set_rootdir(path=None, search_parent_dirs=True):
    global _rootdir

    try:
        _rootdir = os.path.abspath(path)
    except:
        _rootdir = os.getcwd()

    if search_parent_dirs:
        _rootdir = find_rootdir(_rootdir)
    
    return _rootdir

def rootdir():
    return _rootdir
