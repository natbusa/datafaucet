import os

from copy import deepcopy

from ruamel.yaml import YAML

yaml = YAML()
yaml.preserve_quotes = True

from . import utils
from . import project

def resource_unique_name(resource, fullpath_filename):
    if not resource:
        return ''

    unique_name = resource
    if not resource.startswith('.'):
        filename_path = os.path.split(fullpath_filename)[0]
        if 'metadata.yml' not in os.listdir(filename_path):
            raise ValueError(
                'A relative resource "{}" is declared, but there is no metadata file dir : {}'.format(resource,
                                                                                                      filename_path))

        path = utils.breadcrumb_path(filename_path, rootpath=project.rootpath())
        unique_name = '.' + resource if path == '.' else '{}.{}'.format(path, resource)

    return unique_name


def rename_resources(fullpath_filename, params):
    d = params.get('resources', {})
    r = dict()
    for k, v in d.items():
        alias = resource_unique_name(k, fullpath_filename)
        r[alias] = v
    return r

# metadata files are cached once read the first time
_metadata_profiles = {}
  
def _metadata():
    # reference global variable as 'profiles'
    global _metadata_profiles
    profiles =  _metadata_profiles

    if not profiles:
        filenames = utils.get_project_files(
            ext='metadata.yml',
            rootpath=project.rootpath(),
            ignore_dir_with_file='metadata.ignore.yml',
            relative_path=False)

        for filename in filenames:
            f = open(filename, 'r')
            docs = list(yaml.load_all(f))
            for doc in docs:
                profile = doc['profile'] if 'profile' in doc else 'default'
                doc['resources'] = rename_resources(filename, doc)
                profiles[profile] = utils.merge(profiles.get(profile, {}), doc)

        elements = ['resources', 'variables', 'providers', 'engines', 'loggers']

        if 'default' not in profiles.keys():
            profiles['default'] = {'profile': 'default'}

        # empty list for default missing elements:
        for k in elements:
            if k not in profiles['default'].keys():
                profiles['default'][k] = {}

        #defaults to local spark and logging on stdout info
        if not profiles['default']['engines']:
            profiles['default']['engines'] = {'spark': {'context': 'spark'}}
        if not profiles['default']['loggers']:
            profiles['default']['loggers'] = {'stream': {'enable': True, 'severity': 'info'}}

        # inherit from default if not vailable in the profile
        for r in set(profiles.keys()).difference({'default'}):
            for k in elements:
                profiles[r][k] = utils.merge(profiles['default'][k], profiles[r].get(k, {}))

        # inherit from parent if not vailable in the profile
        for r in set(profiles.keys()).difference({'default'}):
            parent = profiles[r].get('inherit')
            if parent:
                for k in elements:
                    profiles[r][k] = utils.merge(profiles[parent][k], profiles[r].get(k, {}))

    return _metadata_profiles

def metadata(profile=None):
    # if nothing passed take the current profile
    profile = profile if profile else project.profile()

    # read-only from _metadata
    md = deepcopy(_metadata()[profile])
    md = utils.render(md)

    # validate!
    utils.validate(md, 'top.yml')
    utils.validate(md['loggers'], 'loggers.yml')

    # return profile metadata
    return md


def metadata_files():
    return utils.get_project_files(
        ext='metadata.yml',
        rootpath=project.rootpath(),
        ignore_dir_with_file='metadata.ignore.yml',
        relative_path=True)
