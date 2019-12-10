import os
import sys

import re
import json
import requests

import ipykernel
from notebook.notebookapp import list_running_servers

_script_path = None


def detect_script_path(f):
    """
    Return the full path of the jupyter notebook.
    """

    # check provided filename (if exists)
    try:
        os.stat(f)
        return os.path.abspath(f)
    except:
        pass

    # fallback 1: interactive shell (ipynb notebook)
    try:
        kernel_filename = ipykernel.connect.get_connection_file()
        kernel_id = re.search('kernel-(.*).json', kernel_filename).group(1)

        for s in list_running_servers():
            url = requests.compat.urljoin(s['url'], 'api/sessions')
            params = {'token': s.get('token', '')}
            response = requests.get(url, params)
            for nn in json.loads(response.text):
                if nn['kernel']['id'] == kernel_id:
                    relative_path = nn['notebook']['path']
                    f = os.path.join(s['notebook_dir'], relative_path)
                    return os.path.abspath(f)
    except:
        pass

    # fallback 2: main file name (python files)
    filename = os.path.abspath(sys.argv[0]) if sys.argv[0] else None
    if filename:
        return filename

    # check if running an interactive ipython sessions
    try:
        __IPYTHON__
        return os.path.join(os.getcwd(), '<ipython-session>')
    except NameError:
        pass

    try:
        __DATALOOF__
        return os.path.join(os.getcwd(), '<datafaucet>')
    except NameError:
        pass

    # nothing found. Use <unknown>
    return os.path.join(os.getcwd(), '<unknown>')


def set_script_path(f=None):
    global _script_path
    _script_path = detect_script_path(f)


def get_script_path(rootdir=None):
    if _script_path is None:
        set_script_path()

    return os.path.relpath(_script_path, rootdir) if rootdir else _script_path


def get_files(ext, rootdir, exclude_dirs=None, ignore_dir_with_file=''):
    if exclude_dirs is None:
        exclude_dirs = []

    lst = list()
    rootdir = os.path.abspath(rootdir)
    for root, dirs, files in os.walk(rootdir, topdown=True):
        for d in exclude_dirs:
            if d in dirs:
                dirs.remove(d)

        if ignore_dir_with_file in files:
            dirs[:] = []
            next
        else:
            for file in files:
                if file.endswith(ext):
                    path = os.path.join(root, file)
                    relative_path = os.path.relpath(path, rootdir)
                    lst.append(relative_path)

    return lst


def get_python_files(rootdir):
    return get_files('.py', rootdir)


def get_metadata_files(rootdir):
    return get_files('metadata.yml', rootdir, None, 'metadata.ignore.yml')


def get_jupyter_notebook_files(rootdir):
    return get_files('.ipynb', rootdir, ['.ipynb_checkpoints'])


def get_dotenv_path(rootdir):
    dotenv_filename = '.env'
    dotenv_path = os.path.join(rootdir, dotenv_filename)
    exists = os.path.isfile(dotenv_path)

    return dotenv_filename if exists else None
