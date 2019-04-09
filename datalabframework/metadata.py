import os
from datetime import datetime
import pytz

from datalabframework import logging
from datalabframework.yaml import yaml
from datalabframework._utils import merge, to_ordered_dict

import json
import jsonschema

from dotenv import load_dotenv
from jinja2 import Environment

# metadata files are cached once read the first time
def read(file_paths=None):
    """
    Return all profiles, stored in a nested dictionary
    Profiles are merged over the list provided of provided metadata files to read. 
    The order in the list of metadata files determines how profile properties are override
    :param file_paths: list of yaml files paths
    :return: dict of profiles
    """
    profiles = {}

    if not file_paths:
        file_paths = []
    
    for filename in file_paths:
        if os.path.isfile(filename):
            with open(filename, 'r') as f:
                try:
                    docs = list(yaml.load_all(f))
                except yaml.YAMLError as e:
                    if hasattr(e, 'problem_mark'):
                        mark = e.problem_mark
                        logging.error("Error loading yml file {} at position: (%s:%s): skipping file".format(filename, mark.line+1, mark.column+1))
                        docs = []
                finally:
                    for doc in docs:
                        doc['profile'] = doc.get('profile', 'default')
                        profiles[doc['profile']] = merge(profiles.get(doc['profile'],{}), doc)

    return profiles

def inherit(profiles):
    """
    Profiles inherit from a default profile.
    Inherit merges each profile with the configuration of the default profile.
    :param profiles: dict of profiles
    :return: dict of profiles
    """

    # inherit from default for all other profiles
    for k in profiles.get('default', {}).keys():
        for p in profiles.keys() - 'default':
            profiles[p][k] = merge(profiles['default'][k], profiles[p].get(k))

    return profiles

def render(metadata,  max_passes=5):
    """
    Renders jinja expressions in the given input metadata.
    jinja templates can refer to the dictionary itself for variable substitution

    :param metadata: profile dict, values may contain jinja templates
    :param max_passes: max number of rendering passes
    :return: profile dict, rendered jinja templates if present
    """

    env = Environment()
    
    def env_func(key, value=None):
        return os.getenv(key, value)
        
    def now_func(tz='UTC', format='%Y-%m-%d %H:%M:%S'):
        dt=datetime.now(pytz.timezone(tz))
        return datetime.strftime(dt, format)
    
    env.globals['env'] = env_func
    env.globals['now'] = now_func
    
    doc = json.dumps(metadata)

    rendered = metadata

    for i in range(max_passes):
        dictionary = json.loads(doc)

        #rendering with jinja
        template = env.from_string(doc)
        doc = template.render(dictionary)

        # all done, or more rendering required?
        rendered = json.loads(doc)
        if dictionary == rendered:
            break

    return rendered

def v(d, schema):
    message=None
    try:
        jsonschema.validate(d, schema)
        return
    except jsonschema.exceptions.ValidationError as e:
        message  = f'{e.message} \n\n## schema path:\n\'{"/".join(e.schema_path)}\'\n\n'
        message += f'## metadata schema definition {"for " + str(e.parent) if e.parent else ""}:'
        message += f'\n{yaml.dump(e.schema)}'
    
    if message:
        raise ValueError(message)
        
def validate_schema(md, schema_filename):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = os.path.abspath(os.path.join(dir_path, 'schemas/{}'.format(schema_filename)))
    with open(filename) as f:
        v(md, yaml.load(f))

def validate(md):

    # validate data structure
    validate_schema(md, 'top.yml')
        
    # _validate_schema(md['loggers'], 'loggers.yml')

    # for d in md['providers']:
    #     _validate_schema(d, 'provider.yml')
    #
    # for d in md['resources']:
    #     _validate_schema(d, 'resource.yml')

    # validate semantics
    providers = md.get('providers', {}).keys()
    for resource_alias, r in md.get('resources',{}).items():
        resource_provider = r.get('provider')
        if resource_provider and resource_provider not in providers:
            print(f'resource {resource_alias}: given provider "{resource_provider}" '
                  'does not match any metadata provider')

def formatted(md):
    keys = (
            'profile',
            'variables',
            ('engine',(
                'type',
                'master'
                )
            ),
            'providers',
            'resources',
            ('loggers',(
                'root',
                ('datalabframework',(
                    'name',
                    'stream',
                    'file',
                    'stdio',
                    'kafka'
                    )
                )
                )
            )
        )
        
    if md.get('variables'):
        md['variables'] = dict(sorted(md['variables'].items()))
            
    return to_ordered_dict(md, keys)

def load(profile='default', metadata_files=None, dotenv_path=None, factory_defaults=True):
    """
    Load the profile, given a list of yml files and a .env filename
    profiles inherit from the defaul profile, a profile not found will contain the same elements as the default profile

    :param profile: the profile to load (default: 'default')
    :param metadata_files: a list of metadata files to read 
    :param dotenv_path: the path of a dotenv file to read
    :param factory_defaults: if True, loads a default configuration
    :return: the loaded metadata profile dict
    """
    # get env variables from .env file
    if dotenv_path and os.path.isfile(dotenv_path):
        load_dotenv(dotenv_path)

    if factory_defaults:
        # get the default metadata configuration file
        dir_path = os.path.dirname(os.path.realpath(__file__))
        default_metadata_file = os.path.abspath(os.path.join(dir_path, 'schemas/default.yml'))
    
        #prepend the default configuration
        file_paths = [default_metadata_file] + metadata_files
    else:
        file_paths = metadata_files
        
    profiles = read(file_paths)
    
    # empty profile if profile not found
    if profile not in profiles.keys():
        if file_paths:
            message = '\nList of loaded metadata files:\n'
            for f in file_paths:
                message += f'  - {f}\n'
            message += '\nList of available profiles:\n'
            for p in profiles.keys():
                message += f'  - {p}\n'   
        raise ValueError(f'Profile "{profile}" not found.\n{message}')

    # read metadata, get the profile, if not found get an empty profile
    profiles = inherit(profiles)
    metadata = profiles[profile]

    # render any jinja templates in the profile
    md = render(metadata)
    
    # validate
    validate(md)
    
    # format
    md  = formatted(md)
    
    return md
