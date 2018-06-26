import os
import yaml

from . import utils
from . import project

DLF_METADATA_FILE = 'DLF_MD_FILE'
DLF_METADATA_RUN  = 'DLF_MD_RUN'

def _hierarchical_resource(filename, params):
    f = utils.relative_filename(filename, rootpath=project.rootpath())
    p = f.rfind('/')
    resource_path = f[:p+1].replace('/','.') if p>0 else ''

    d = params.get('resources', {})
    r = dict()
    for k,v in d.items():
        if k.startswith('.'):
            r[k] = v
        else:
            alias_abs = '.{}{}'.format(resource_path,k)
            r[alias_abs] = v
    return r

def metadata(all_runs=False):
    v = os.getenv(DLF_METADATA_FILE)
    mf = utils.get_project_files(ext='metadata.yml', rootpath=project.rootpath(), ignore_dir_with_file='metadata.ignore.yml', relative_path=False)
    filenames = [v] if v else mf

    runs = {}

    for filename in filenames:
        f = open(filename,'r')
        docs = list(yaml.load_all(f))
        for params in docs:
            k = params['run'] if 'run' in params else 'default'
            params['resources'] = _hierarchical_resource(filename, params)
            runs[k] = utils.merge(runs.get(k,{}), params)

    v = os.getenv(DLF_METADATA_RUN)
    r = v if v else 'default'

    return runs if all_runs else runs[r]

def metadata_info():
    mf = utils.get_project_files(
        ext='metadata.yml',
        rootpath=project.rootpath(),
        ignore_dir_with_file='metadata.ignore.yml',
        relative_path=True)
    rootpath=project.rootpath()
    runs = metadata(True).keys()
    info = {'files': mf, 'runs': list(runs), 'rootpath': rootpath}
    return info
