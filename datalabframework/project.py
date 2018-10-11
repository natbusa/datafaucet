import os
import io
import sys
import types
import subprocess
import getpass
from urllib.parse import urljoin

import json
import re
import ipykernel
import requests
from IPython.core.interactiveshell import InteractiveShell
from notebook.notebookapp import list_running_servers

import nbformat
from . import utils

def _rootpath(rootfile='__main__.py'):
    path = os.getcwd()
    while True:
        try:
            ls = os.listdir(path)
            if rootfile in ls:
                return os.path.abspath(path)
            else:
                path += '/..'
        except:
            break

    #fallback: current dir
    path = os.getcwd()
    return path

def _get_filename(f=None):
    """
    Return the full path of the jupyter notebook.
    """
    # when running not in and interactive shell,
    # just get the filename of the main script

    # check provided filename (if exists)
    try:
        os.stat(f)
        return os.path.abspath(f)
    except:
        pass

    # fallback 1: main file name (python files)
    try:
        import __main__ as main
        return os.path.abspath(os.path.basename(main.__file__))
    except:
        pass

    #fallback 2: interactive shell (ipynb notebook)
    try:
        kernel_filename = ipykernel.connect.get_connection_file()
        kernel_id = re.search('kernel-(.*).json',kernel_filename).group(1)

        for s in list_running_servers():
            url =urljoin(s['url'], 'api/sessions')
            params={'token': s.get('token', '')}
            response = requests.get(url,params)
            for nn in json.loads(response.text):
                if nn['kernel']['id'] == kernel_id:
                    relative_path = nn['notebook']['path']
                    f = os.path.join(s['notebook_dir'], relative_path)
                    return os.path.abspath(f)
    except:
        pass

    #check if running an interactive ipython sessions
    try:
        __IPYTHON__
        return os.path.join(os.getcwd(),'<ipython-session>')
    except NameError:
        pass

    try:
        __DATALABFRAMEWORK__
        return os.path.join(os.getcwd(),'<datalabframework>')
    except NameError:
        pass

    #nothing found. Use a fake name
    return os.path.join(os.getcwd(),'<unknown>')

def _find_notebook(fullname, paths=None):
    """find a notebook, given its fully qualified name and an optional path

    This turns "foo.bar" into "foo/bar.ipynb"
    and tries turning "Foo_Bar" into "Foo Bar" if Foo_Bar
    does not exist.
    """
    name = fullname.rsplit('.', 1)[-1]
    if not paths:
        paths = ['']
    for d in paths:
        nb_path = os.path.join(d, name + ".ipynb")
        if os.path.isfile(nb_path):
            return nb_path
        # let import Notebook_Name find "Notebook Name.ipynb"
        nb_path = nb_path.replace("_", " ")
        if os.path.isfile(nb_path):
            return nb_path

class NotebookLoader(object):
    """Module Loader for Jupyter Notebooks"""
    def __init__(self, path=None):
        if not InteractiveShell:
            return

        self.shell = InteractiveShell.instance()
        self.path = path

    def load_module(self, fullname):
        if not InteractiveShell:
            return

        """import a notebook as a module"""
        path = _find_notebook(fullname, self.path)

        print ("importing Jupyter notebook from %s" % path)

        # load the notebook object
        with io.open(path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, 4)

        # create the module and add it to sys.modules
        # if name in sys.modules:
        #    return sys.modules[name]
        mod = types.ModuleType(fullname)
        mod.__file__ = path
        mod.__loader__ = self
        mod.__dict__['get_ipython'] = get_ipython
        sys.modules[fullname] = mod

        # extra work to ensure that magics that would affect the user_ns
        # actually affect the notebook module's ns
        save_user_ns = self.shell.user_ns
        self.shell.user_ns = mod.__dict__

        try:
          for cell in nb.cells:
            if cell['cell_type'] == 'code' and cell['source'].startswith('#EXPORT'):
                # transform the input to executable Python
                code = self.shell.input_transformer_manager.transform_cell(cell.source)

                # run the code in themodule
                exec(code, mod.__dict__)
        finally:
            self.shell.user_ns = save_user_ns
        return mod

class NotebookFinder(object):
    """Module finder that locates Jupyter Notebooks"""
    def __init__(self):
        self.loaders = {}

    def find_module(self, fullname, path=None):
        nb_path = _find_notebook(fullname, path)
        if not nb_path:
            return

        key = path
        if path:
            # lists aren't hashable
            key = os.path.sep.join(path)

        if key not in self.loaders:
            mod = NotebookLoader(path)
            if mod:
                self.loaders[key] = NotebookLoader(path)

        return self.loaders[key]

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Config(metaclass=Singleton):
    _rootpath = None
    _filename = None
    _workdir  = None
    _profile  = None

    def _init_sys_paths(self):
        # add roothpath to the list of python sys paths
        if self._rootpath not in sys.path:
            sys.path.append(self._rootpath)

        # register hook for loading ipynb files
        if 'NotebookFinder' not in str(sys.meta_path):
            sys.meta_path.append(NotebookFinder())

    def __init__(self, cwd=None, filename=None, profile=None):
        #set workdir
        try:
            os.chdir(cwd)
        except:
            pass
        finally:
            self._workdir = os.getcwd()

        # set filename
        self._filename = _get_filename(filename)

        # set metadata profile
        self._profile = profile

        # set rootpath
        self._rootpath = _rootpath()

        # set python paths
        self._init_sys_paths()

        # get git data
        self._repository = utils.repo_data()

        # get user
        self._username = getpass.getuser()

    def filename(self, relative_path=True):
        rel_filename = utils.relative_filename(self._filename, self._rootpath)
        return rel_filename if relative_path else self._filename

    def rootpath(self):
        return self._rootpath

    def workdir(self):
        return self._workdir

    def repository(self):
        return self._repository

    def username(self):
        return self._username

    def profile(self, p=None):
        # change only if not defined yet,
        if p and self._profile and p != self._profile:
            print("can only set the profile once per script. (current profile is '{}')".format(self._profile))

        # change only if not defined yet,
        if p and self._profile is None:
            self._profile = p

        return self._profile if self._profile else 'default'

def rootpath():
    c = Config()
    return c.rootpath()

def workdir():
    c = Config()
    return c.workdir()

def filename(relative_path=True):
    c = Config()
    return c.filename(relative_path)

def profile(p=None):
    c = Config()
    return c.profile(p)

def info():
    k = ['profile','filename','rootpath', 'workdir']
    v = [eval(x+'()') for x in k]
    return dict(zip(k,v))

def notebooks():
    return utils.get_project_files(ext='.ipynb', exclude_dirs=['.ipynb_checkpoints'])

def repository():
    c = Config()
    return c.repository()

def username():
    c = Config()
    return c.username()
