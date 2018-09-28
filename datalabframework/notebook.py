import os
import datetime
import nbformat

import json
import re

from . import utils
from . import project

def statistics(filename):
    data = {}
    with open(utils.absolute_filename(filename, project.rootpath())) as f:
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