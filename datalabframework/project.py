import os
import io
import sys
import types
import subprocess

import json
import re
import requests
from urllib.parse import urljoin

import ipykernel
from IPython.core.interactiveshell import InteractiveShell
from notebook.notebookapp import list_running_servers

import nbformat
from . import utils

def _rootpath(rootfile='__main__.py'):
    path = '.'
    while True:
        try:
            ls = os.listdir(path)
            if rootfile in ls:
                return os.path.abspath(path)
        except:
            break
        path += '/..'

    path = os.getcwd()
    return path

def _get_current_notebook():
    """
    Return the full path of the jupyter notebook.
    """

    # when running not in and interactive shell,
    # just get the filename of the main script
    try:
        import __main__ as main
        filename = os.path.basename(main.__file__)
    except:
        filename = ''

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
                    filename = os.path.join(s['notebook_dir'], relative_path)
    except:
        pass

    return os.path.abspath(filename)

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

# Singleton/ClassVariableSingleton.py
class Singleton(object):
    _instance = None
    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_)

        class_._instance.__init__(*args, **kwargs)
        return class_._instance

class Config(Singleton):
    _rootpath = None
    _filename = None
    _cwd = None

    def __init__(self, cwd=None, filename=None):
        if cwd and self._cwd != cwd:
            self._cwd = cwd
            self._filename = None
            self._rootpath = None

            os.chdir(cwd)

        if filename and not self._filename:
            self._filename = os.path.abspath(filename)

        if not self._filename:
            self._filename = _get_current_notebook()

        if not self._cwd:
            self._cwd = os.getcwd()

        if not self._rootpath:
            self._rootpath = _rootpath()

            if self._rootpath not in sys.path:
                sys.path.append(self._rootpath)

            # register hook for loading ipynb files
            sys.meta_path.append(NotebookFinder())

    def filename(self, relative_path=True):
        rel_filename = utils.relative_filename(self._filename, self._rootpath)
        return rel_filename if relative_path else self._filename

    def rootpath(self):
        return self._rootpath

    def working_dir(self):
        return self._cwd

def rootpath():
    c = Config()
    return c.rootpath()

def filename(relative_path=True):
    c = Config()
    return c.filename(relative_path)
