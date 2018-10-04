import os
import yaml
import copy

from . import utils
from . import project

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

def metadata(run=None, all_runs=False):
    #if nothing passed take the current run
    run = run if run else project.workrun()

    filenames = utils.get_project_files(
        ext='metadata.yml',
        rootpath=project.rootpath(),
        ignore_dir_with_file='metadata.ignore.yml',
        relative_path=False)

    runs = {}
    for filename in filenames:
        f = open(filename,'r')
        docs = list(yaml.load_all(f))
        for params in docs:
            if 'run' not in params:
                params['run'] = 'default'
            params['resources'] = rename_resources(filename, params)
            k = params['run']
            runs[k] = utils.merge(runs.get(k,{}), params)

    elements = ['resources','providers', 'engines', 'loggers']

    # empty list for default missing elements:
    for k in elements:
        if k not in runs['default'].keys():
            runs['default'][k] = {}

    # inherit from default if not vailable in the run
    for r in set(runs.keys()).difference({'default'}):
        for k in elements:
            if k not in runs[r] or not runs[r][k]:
                runs[r][k] = copy.deepcopy(runs['default'][k])

    # rendering of jinja constructs
    runs = utils.render(runs)

    return runs if all_runs else runs[run]

def metadata_files():
    return utils.get_project_files(
        ext='metadata.yml',
        rootpath=project.rootpath(),
        ignore_dir_with_file='metadata.ignore.yml',
        relative_path=True)
