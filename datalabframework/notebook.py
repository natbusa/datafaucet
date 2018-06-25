import os
import datetime
import nbformat

import json
import re

import requests

try:
    import ipykernel
    from notebook.notebookapp import list_running_servers
except:
    ipykernel=None

try:  # Python 3
    from urllib.parse import urljoin
except ImportError:  # Python 2
    from urlparse import urljoin

from . import utils

def _get_filename():
    """
    Return the full path of the jupyter notebook.
    """
    if not ipykernel:
        return ''

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

def get_filename(relative_path=True):
    f = _get_filename()
    return utils.relative_filename(f) if relative_path else f

def statistics(filename):
    data = {}
    with open(utils.absolute_filename(filename)) as f:
        data = json.load(f)

    #todo: check for filetype
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

def list_all():
    return utils.get_project_files(ext='.ipynb', exclude_dirs=['.ipynb_checkpoints'])
