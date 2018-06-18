import os
import datetime
import nbformat

import json
import re
import ipykernel
import requests

try:  # Python 3
    from urllib.parse import urljoin
except ImportError:  # Python 2
    from urlparse import urljoin

try:  # Python 3
    from notebook.notebookapp import list_running_servers
except ImportError:  # Python 2
    import warnings
    from IPython.utils.shimmodule import ShimWarning
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=ShimWarning)
        from IPython.html.notebookapp import list_running_servers

from nbconvert.preprocessors import ClearOutputPreprocessor

from .project import rootpath

def lrchop(s, b='', e='.ipynb'):
    if s.startswith(b) and len(b)>0:
        s = s[len(b):]
    if s.endswith(e) and len(e)>0:
        s = s[:-len(e)]
    return s

def get_notebook_filename():
    """
    Return the full path of the jupyter notebook.
    """
    kernel_id = re.search('kernel-(.*).json',
                          ipykernel.connect.get_connection_file()).group(1)
    servers = list_running_servers()
    for ss in servers:
        response = requests.get(urljoin(ss['url'], 'api/sessions'),
                                params={'token': ss.get('token', '')})
        for nn in json.loads(response.text):
            if nn['kernel']['id'] == kernel_id:
                relative_path = nn['notebook']['path']
                return os.path.join(ss['notebook_dir'], relative_path)

def filename(s=None, ext='.ipynb'):
    if not s:
        s = get_notebook_filename()

    s = lrchop(s,rootpath(), ext).lstrip('/')
    (path, name) = os.path.split(s)
    return (path, name)

def list_all(path=None, removelist=[]):

    if not path:
        path  = rootpath()

    if not path:
        path  = '.'

    lst = list()
    for root, dirs, files in os.walk(path):

        # don't visit .ipynb_checkpoints directories
        if '.ipynb_checkpoints' in dirs:
            dirs.remove('.ipynb_checkpoints')

        for file in files:
            if file.endswith("nbconvert.ipynb"):
                continue

            if file.endswith(".ipynb"):
                basedir = root[len(path):]
                filename = os.path.join(basedir, file).lstrip('/')
                if not any([filename.startswith(x) for x in removelist]):
                    lst.append(filename)
    return lst

def statistics(notebook_filename):
    data = {}
    with open(notebook_filename) as f:
        data = json.load(f)

    stats = {'cells': len(data['cells'])}

    h = dict()
    for c in data['cells']:
        count = h.get(c['cell_type'], 0)
        h[c['cell_type']] = count + 1
    stats.update(h)

    error = {'ename': None, 'evalue': None}
    for c in data['cells']:
        if c['cell_type']=='code':
            for o in c['outputs']:
                if o['output_type'] == 'error':
                    error = {'ename': o['ename'], 'evalue': o['evalue']}
                    break
    stats.update(error)

    count =0
    for c in data['cells']:
        if c['cell_type']=='code' and c['execution_count']:
            count +=1
    stats.update({'executed': count})

    return stats

def clear(notebook_filename, nbconvert_filename=None):
    with open(notebook_filename, 'r') as fh:
        nb = nbformat.read(fh, 4)

    # check outputs of all the cells
    preprocessor = ClearOutputPreprocessor()
    clear_nb = preprocessor.preprocess(nb, {})[0]

    # clear metadata
    clear_nb.metadata = {}

    # if output notebook not defined, overwrite notebook
    if not nbconvert_filename:
        nbconvert_filename = notebook_filename

    # write the notebook back to disk
    with open(nbconvert_filename, 'w') as fh:
        nbformat.write(clear_nb, fh, 4)

def convert(notebook_filename, nbconvert_filename=None):
    #todo: convert the notebook to python
    return
