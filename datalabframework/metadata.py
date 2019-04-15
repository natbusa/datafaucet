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

loaded_md_files = []
profiles = {}

# metadata files are cached once read the first time
def read(file_paths=None):
    """
    Return all profiles, stored in a nested dictionary
    Profiles are merged over the list provided of provided metadata files to read. 
    The order in the list of metadata files determines how profile properties are override
    :param file_paths: list of yaml files paths
    :return: dict of profiles
    """
    global loaded_md_files, profiles
    
    # empty profiles, before start reading 
    profiles = {}

    if not file_paths:
        file_paths = []
    
    loaded_md_files = []
    for filename in file_paths:
        if os.path.isfile(filename):
            with open(filename, 'r') as f:
                try:
                    docs = list(yaml.load_all(f))
                    loaded_md_files.append(filename)
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
        for p in set(profiles.keys()) - {'default'}:
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
    msg_error=None
    try:
        jsonschema.validate(d, schema)
        return
    except jsonschema.exceptions.ValidationError as e:
        msg_error  = f'{e.message} \n\n## schema path:\n'
        msg_error += f'\'{"/".join(e.schema_path)}\'\n\n'
        msg_error += f'## metadata schema definition '
        msg_error += f'{"for " + str(e.parent) if e.parent else ""}:'
        msg_error += f'\n{yaml.dump(e.schema)}'
        
    if msg_error:
        raiseException(msg_error)
        
def validate_schema(md, schema_filename):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = os.path.abspath(os.path.join(dir_path, 'schemas/{}'.format(schema_filename)))
    with open(filename) as f:
        v(md, yaml.load(f))

def validate(md):

    # validate data structure
    validate_schema(md, 'top.yml')
        
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
                'master',
                'jobname',
                'timezone',
                ('submit',(
                    'detect',
                    'jars',
                    'packages',
                    'py-files',
                )),
                'config',
                )
            ),
            'providers',
            'resources',
            ('loggers',(
                ('root',('severity',)),
                ('datalabframework',(
                    'name',
                    ('stream',(
                        'severity',
                        'enable',
                    )),
                    ('stdout',(
                        'severity',
                        'enable',
                    )),
                    ('file',(
                        'severity',
                        'enable',
                        'path',
                    )),
                    ('kafka',(
                        'severity',
                        'enable',
                        'hosts',
                        'topic',
                    ))
                ))
            )),
        )
    
    d = to_ordered_dict(md, keys)
    
    if d['variables']:
        d['variables'] = dict(sorted(d['variables'].items()))
    
    return d

def debugMetadataFiles():
    message = '\nList of loaded metadata files:\n'
    if loaded_md_files:
        for f in loaded_md_files:
            message += f'  - {f}\n'
    else:
        message += 'None'
        
    return message

def debugProfiles():
    message = '\nList of available profiles:\n'
    if profiles:
        for f in profiles.keys():
            message += f'  - {f}\n'
    else:
        message += 'None'
        
    return message

def raiseException(message=''):
    message += '\n'
    message += debugMetadataFiles()
    message += debugProfiles()
    raise ValueError(message)
    
def load(profile='default', metadata_files=None, dotenv_path=None):
    """
    Load the profile, given a list of yml files and a .env filename
    profiles inherit from the defaul profile, a profile not found will contain the same elements as the default profile

    :param profile: the profile to load (default: 'default')
    :param metadata_files: a list of metadata files to read 
    :param dotenv_path: the path of a dotenv file to read
    :return: the loaded metadata profile dict
    """
    # get env variables from .env file
    if dotenv_path and os.path.isfile(dotenv_path):
        load_dotenv(dotenv_path)
    
    profiles = read(metadata_files)
    
    # empty profile if profile not found
    if profile not in profiles.keys():
        raiseException(f'Profile "{profile}" not found.')

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
