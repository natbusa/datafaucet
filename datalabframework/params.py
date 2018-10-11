import os
import yaml
import copy

from . import utils
from . import project

def resource_unique_name(resource, fullpath_filename):
    if not resource:
        return ''

    unique_name = resource
    if not resource.startswith('.'):
        filename_path = os.path.split(fullpath_filename)[0]
        if not 'metadata.yml' in os.listdir(filename_path):
            raise ValueError('A relative resource "{}" is declared, but there is no metadata file dir : {}'.format(resource, filename_path))

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

def metadata(profile=None, all=False):
    #if nothing passed take the current profile
    profile = profile if profile else project.profile()

    filenames = utils.get_project_files(
        ext='metadata.yml',
        rootpath=project.rootpath(),
        ignore_dir_with_file='metadata.ignore.yml',
        relative_path=False)

    profiles = {}
    for filename in filenames:
        f = open(filename,'r')
        docs = list(yaml.load_all(f))
        for params in docs:
            if 'profile' not in params:
                params['profile'] = 'default'
            params['resources'] = rename_resources(filename, params)
            k = params['profile']
            profiles[k] = utils.merge(profiles.get(k,{}), params)

    elements = ['resources','variables', 'providers', 'engines', 'loggers']

    # empty list for default missing elements:
    for k in elements:
        if k not in profiles['default'].keys():
            profiles['default'][k] = {}

    # inherit from default if not vailable in the profile
    for r in set(profiles.keys()).difference({'default'}):
        for k in elements:
            if k not in profiles[r] or not profiles[r][k]:
                profiles[r][k] = copy.deepcopy(profiles['default'][k])

    # rendering of jinja constructs
    profiles = utils.render(profiles)

    #validate all all_profiles
    for k in profiles.keys():
        utils.validate(profiles[k], 'top.yml')

    return profiles if all else profiles[profile]

def metadata_files():
    return utils.get_project_files(
        ext='metadata.yml',
        rootpath=project.rootpath(),
        ignore_dir_with_file='metadata.ignore.yml',
        relative_path=True)
