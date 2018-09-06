import os
import yaml

from . import utils
from . import project

DLF_METADATA_FILE = 'DLF_MD_FILE'
DLF_METADATA_RUN  = 'DLF_MD_RUN'

def resource_unique_name(resource, fullpath_filename):
    unique_name = resource

    if not resource.startswith('.'):
        filename_path = os.path.split(fullpath_filename)[0]
        if not 'metadata.yml' in os.listdir(filename_path):
            raise ValueError('No metadata file, in the current dir')

        path = utils.breadcrumb_path(filename_path, rootpath=project.rootpath())
        unique_name = '.'+resource if path=='.' else '{}.{}'.format(path, resource)

    return unique_name

def rename_resources(fullpath_filename, params):
    d = params.get('resources', {})
    r = dict()
    for k,v in d.items():
        alias = resource_unique_name(k,fullpath_filename)
        r[alias] = v
    return r

def metadata(all_runs=False):
    v = os.getenv(DLF_METADATA_FILE)
    mf = utils.get_project_files(
        ext='metadata.yml',
        rootpath=project.rootpath(),
        ignore_dir_with_file='metadata.ignore.yml',
        relative_path=False)

    filenames = [v] if v else mf

    runs = {}
    for filename in filenames:
        f = open(filename,'r')
        docs = list(yaml.load_all(f))
        for params in docs:
            k = params['run'] if 'run' in params else 'default'
            params['resources'] = rename_resources(filename, params)
            runs[k] = utils.merge(runs.get(k,{}), params)

    v = os.getenv(DLF_METADATA_RUN)
    r = v if v else 'default'

    # rendering of jinja constructs
    runs = utils.render(runs)

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
