"""
return a metadata object which provides just one method:
:return: a Metadata object

Notes about metadata configurations:

#### Metadata files

1) Metadata files are merged up, so you can split the information in
   multiple files as long as they end with `metadata.yml`.
   For example: `metadata.yml`, `abc.metadata.yaml`, `abc_metadata.yml`
   are all valid metadata file names.

2) All metadata files in all subdirectories from the project root directory are loaded,
   unless the directory contains a file `metadata.ignore.yml`

3) Metadata files can provide multiple profile configurations,
   by separating each profile configuration with as `bare documents` wihtin the same yaml file
   by separating the configuration with a line containing three hyphens (`---`)
   (see https://yaml.org/spec/1.2/spec.html#YAML)

4) Each metadata profile, can be broken down in multiple yaml files,
   When loading the files all configuration belonging to the same profile with be merged.

5) All metadata profiles inherit the settings from profile 'default'

#### Metadata sections

Metadata files are composed of 6 sections:
    - profile
    - variables
    - providers
    - resources
    - engine
    - logging

##### Profile name
A metadata configuration supports multiple profiles.
By default the following profiles are present in the configuration,
when loading the metadata with the argument `factory_defaults=True`

 - default
 - prod
 - stage
 - test
 - test

You can define and use the custom profiles.
by loading a different profile you can define different configuratioon for your data resources,
without having to modify your code. For instance, you cat setup the files to be saves on local
disk for testing and in hdfs for production, as described in this snippet below:

```
    ---
    profile: default
    providers:
        processed_data:
            service: local
            path: data
            format: parquet
    ---
    profile: prod
    providers:
        processed_data:
            service: hdfs
            hostname: hdfs-namenode
            path: /prod/data
    ---
    profile: test
```

In the above example, the profiles `test` and `default` share the same configuration,
while the profile `prod` defined the provider alias `processed_data` as an hdfs location.

You can also use profiles to define different options configurations for the spark engine
or different logging options. Here below an example of a default configuration which uses
a local spark setup in test/dev while using a spark cluster for prod and stage profiles

```
    ---
    profile: default
    engine:
        type: spark
        master: local[*]
    ---
    profile: prod
    engine:
        type: spark
        master: spark://spark-prod-cluster:17077
    ---
    profile: stage
    engine:
        type: spark
        master: spark://spark-stage-cluster:17077
    ---
    profile: test
```

##### Profile Variables

The variable section in the profile allows you to compose information and
reuse them in other part of the configuration.  The datafaucet yaml files
support jinja2 templates for variable substitution. The template rendering
is only performed once upon project load.

Here below an example of a variable section and
how to use it for the rest of the configuration:
```
    ---
    profile: default
    variables:
      a: hello
      b: "{{ variables.a}} world"
      c: "{{ env('SHELL') }}"
      d: "{{ env('ENV_VAR_NOT_DEFINED', 'foo'}}"
      e: "{{ now() }}"
      f: "{{ now(tz='UTC', format='%Y-%m-%d %H:%M:%S') }}"

      my_string_var: "Hi There!"
      my_env_var: "{{ env('DB_USERNAME', 'guest') }}"
      my_concat_var: "{{ engine.type }} running at {{ engine.master }}"

    ---
```

The above metadata profile will be rendered as:

```
    ---
    profile: default
    variables:
        a: hello
        b: hello world
        c: /bin/bash
        d: foo
        e: '2019-03-27 08:42:00'
        f: '2019-03-27'
        my_string_var: Hi There!
        my_env_var: guest
        my_concat_var: spark running at local[*]
    ---
```

Note that:

 - variables can be defined in multiple profiles

 - variables section in a give profile always
   inherit the variable from the `default` profile

 - a maximum of 5 rendering passes if allowed

 - values including a jinja template context must alwasy be quoted.
   As is my_var: "{{ <my_jinja-template > }}"

Accessing configuration values in a jinja template:

Yaml object values can be referenced in the jinja template using the . notation.
To access the data item, provide the path from the root of the profile.
For instance the provider `processed_data` format in the example above can be referenced as:
    `providers.processed_data.format`

Jinja rendering operations:
Please refer to <url> for a list of operators on jinja variables

Metadata Jinja functions:
On top of the default set of operations,
two functions can be used inside a jinja rendering context:

`def env(env_var, default_value='null')`
renders in the template the value of environment variable `env_var` or null if not available.
This function can be useful (also in combination with a .env file), to avoid hard-coding
passwords and other login/auth data in the metadata configuration. Note that is setup is meant
for convinence and not for security. Example:

    my_env_var: "{{ env('DB_USERNAME', 'guest') }}"

`def now(tz='UTC', format='%Y-%m-%d %H:%M:%S')`
renders the system current datetime value, optionally a different timezone and string formatting
option can be added. This function can be useul if you want to execute code on a time window
relative to the current time. Example:

    utc_now: "{{ now(tz='UTC', format='%Y-%m-%d %H:%M:%S') }}"


##### Profile Providers

A provider is a service which allows you to load and save data. The datafaucet
extend the spark load save API calls by decoupling the provider configuration from the code.
The `providers` section in the metadata allow you to define a arbitrary number of providers.
A provider are declared as an alias defining a set of properties. See example below:

```
    profile: default

    providers:
        my_provider:
            service: hdfs
            hostname: hdfs-namenode
            path: /foo
            format: parquet
```

This table provides a list of valid properties you can defined for a given provider alias:

`service`:
    The service which is going to be use for load/save data.
    Supported services:
      - minio
      - hdfs
      - local
      - mysql
      - postgres
      - oracle
      - mssql
      - sqlite

`format`:
    The format used for reading and writing data
    Default is 'parquet' for all filesystem and object store services.
    For other type of services, such as databases, this property is ignored.
    Supported formats:
      - jdbc
      - nosql
      - csv
      - parquet
      - json
      - jsonl

`host`, `hostname`:
The name of the host providing the service
Default is 127.0.0.1

`port`:
The port number of the host providing the service
The default port depends on the service according to the following table:

    - hdfs: 8020
    - mysql: 3306
    - postgres: 5432
    - mssql: 1433
    - oracle: 1521
    - elastic: 9200
    - minio:9000

`database`:
The database name from the selected jdbc service

`path`:
The root path used to save/load data resources. If the path is a fully qualified url
such as (`hdfs://data.cluster.local/foo/bar`), it will be used straight away.

Otherwise the url will be assembled using the following properties:
   - `service`
   - `host`
   - `port`
   - `database`
   - `path`

When the service is a jdbc connection,
the property `path` of the provider can be used to refer to the database name

`username`,
`password`,
The credential for authenticating for the given providers

`cache`:
Cache the data, before saving or after loading

`date_column`:
define a column in the dataframe to be the date column (for faster read/write)

`date_start`:
filter data according to this start date/datetime for the `date_column`

`date_end`:
filter data according to this end date/datetime for the `date_column`

`date_window`:
in combination with either `date_end` or `date_start`
it defines a filter interval for the `date_column`.
If defined and valid, this is implicitely applied when loading and saving data.

`date_partition`:
`update_column`:
`hash_column`:
`state_column`:
`hash_column`:
Add special columns to the dataframe.

`options`:
Extra options, as defined in the selected engine for load/save

##### Profile Resources

Resources are defined in the same way as providers, with the difference that
but you can specify a provider parameter. In this case the resource inherits
the provider's configuration. By sharing most of the configuration for the resources as
common provider configuration, you can keep resource configuration very lean and coincise.

`path`
When the service is a jdbc connection,
the property `path` of a resource can be used to refer to the table name

When the service is a file system or block storage or an object store,
The reource path is appended/concatenated to the provider's path

`provider`:
The provider alias configuration, which is used as base configuration fro this resource.


##### Profile Engine

This section defines the engine configurations to process data.
The following properties can be defined:

`type`:
Engine type. Currently supports is limited to the option `spark`

`master`:
The url of the spark master (e.g. `spark://23.195.26.187:7077`)

`jobname`:
The name of the application to be reported in the spark-ui and history server
Default is constructed using the git repository name (if available) and the profile name

`timezone`:
This option allows spark to interpret the datetime data as belonging
to a different timezone than the one provided by the machine defaults.

When set to `naive` will interpret each datetime object as 'naive',
no timezone translation will be executed. This option is equivalent
to setting the `timezone` parameter to 'UTC'

`submit`:
A section which allows to definea and add a number of files during the engine's initalization.
Files are declared as belonging to the following groups

 - `jars`
 - `packages`
 - `py-files`

`config`:
 A list of custom configurations, defined as key, value pairs.
 For example, check out the list of valid Spark 2.4.0 configurations as provided at
 https://spark.apache.org/docs/2.4.0/configuration.html

##### Profile Logging
This section define the logging configuration.
"""
import os
from datetime import datetime
import pytz

from datafaucet import paths
from datafaucet import files
from datafaucet import logging

from datafaucet.yaml import yaml, YamlDict
from datafaucet.utils import Singleton, abspath, merge, to_ordered_dict

import json
import jsonschema

from dotenv import load_dotenv
from jinja2 import Environment


class Metadata(metaclass=Singleton):
    def __init__(self):
        self._info = {
            'files': dict(),
            'profiles': set(),
            'active': None
        }
        self._profile = dict()

    # metadata files are cached once read the first time
    def read(self, file_paths=None):
        """
        Return all profiles, stored in a nested dictionary
        Profiles are merged over the list provided of provided metadata files to read.
        The order in the list of metadata files determines how profile properties are override
        :param file_paths: list of yaml files paths
        :return: dict of profiles
        """

        # empty profiles, before start reading
        profiles = {}

        if not file_paths:
            file_paths = []

        self._info['files'] = []
        for filename in file_paths:
            if os.path.isfile(filename):
                with open(filename, 'r') as f:
                    try:
                        docs = list(yaml.load_all(f))
                        self._info['files'].append(filename)
                    except yaml.YAMLError as e:
                        if hasattr(e, 'problem_mark'):
                            mark = e.problem_mark
                            logging.error(
                                "Error loading yml file {} at position: (%s:%s): skipping file".format(filename,
                                                                                                       mark.line + 1,
                                                                                                       mark.column + 1))
                            docs = []
                    finally:
                        for doc in docs:
                            doc['profile'] = doc.get('profile', 'default')
                            profiles[doc['profile']] = merge(profiles.get(doc['profile'], {}), doc)

        self._info['profiles'] = sorted(list(profiles.keys()))

        return profiles

    def inherit(self, profiles):
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

    def render(self, metadata, max_passes=5):
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
            dt = datetime.now(pytz.timezone(tz))
            return datetime.strftime(dt, format)

        env.globals['env'] = env_func
        env.globals['now'] = now_func

        doc = json.dumps(metadata)

        rendered = metadata

        for i in range(max_passes):
            dictionary = json.loads(doc)

            # rendering with jinja
            template = env.from_string(doc)
            doc = template.render(dictionary)

            # all done, or more rendering required?
            rendered = json.loads(doc)
            if dictionary == rendered:
                break

        return rendered

    def v(self, d, schema):
        try:
            jsonschema.validate(d, schema)
            return
        except jsonschema.exceptions.ValidationError as e:
            msg_error = f'{e.message} \n\n## schema path:\n'
            msg_error += f'\'{"/".join(e.schema_path)}\'\n\n'
            msg_error += f'## metadata schema definition '
            msg_error += f'{"for " + str(e.parent) if e.parent else ""}:'
            msg_error += f'\n{yaml.dump(e.schema)}'
            self.raiseException(msg_error)

    def validate_schema(self, md, schema_filename):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        filename = os.path.abspath(os.path.join(dir_path, 'schemas/{}'.format(schema_filename)))
        with open(filename) as f:
            self.v(md, yaml.load(f))

    def validate(self, md):
        # validate data structure
        self.validate_schema(md, 'top.yml')

        # for d in md['providers']:
        #     _validate_schema(d, 'provider.yml')
        #
        # for d in md['resources']:
        #     _validate_schema(d, 'resource.yml')

        # validate semantics
        providers = md.get('providers', {}).keys()
        for resource_alias, r in md.get('resources', {}).items():
            resource_provider = r.get('provider')
            if resource_provider and resource_provider not in providers:
                print(f'resource {resource_alias}: given provider "{resource_provider}" '
                      'does not match any metadata provider')

    def formatted(self, md):
        keys = (
            'profile',
            'variables',
            ('engine', (
                'type',
                'master',
                'timezone',
                'repositories',
                'jars',
                'packages',
                'files',
                'conf',
                'detect'
                ),
            ),
            'providers',
            'resources',
            ('logging', (
                'level',
                'stdout',
                'file',
                'kafka'
            )),
        )

        d = to_ordered_dict(md, keys)

        if d['variables']:
            d['variables'] = dict(sorted(d['variables'].items()))

        return d

    def debug_metadata_files(self):
        message = '\nList of loaded metadata files:\n'
        if self._info['files']:
            for f in self._info['files']:
                message += f'  - {f}\n'
        else:
            message += 'None'

        return message

    def debug_profiles(self):
        message = '\nList of available profiles:\n'
        if self._info['profiles']:
            for f in self._info['profiles']:
                message += f'  - {f}\n'
        else:
            message += 'None'

        return message

    def raiseException(self, message=''):
        message += '\n'
        message += self.debug_metadata_files()
        message += self.debug_profiles()
        raise ValueError(message)

    def load(self, profile_name='default', metadata_files=None, dotenv_path=None, parameters=None):
        """
        Load the profile, given a list of yml files and a .env filename
        profiles inherit from the defaul profile, a profile not found will contain the same elements as the default profile

        :param profile_name: the profile to load (default: 'default')
        :param metadata_files: a list of metadata files to read
        :param dotenv_path: the path of a dotenv file to read
        :param parameters: optional dict, merged with metadata variables
        :return: the loaded metadata profile dict
        """

        # get metadata by scanning rootdir, if no list is provided
        if metadata_files is None:
            metadata_files = []

            # defaults metadata
            dir_path = os.path.dirname(os.path.realpath(__file__))
            metadata_files += abspath(['schemas/default.yml'], dir_path)

            # project metadata
            metadata_files += abspath(files.get_metadata_files(paths.rootdir()), paths.rootdir())

        # get dotenv_path by scanning rootdir, if no dotenv file is provided
        if dotenv_path is None:
            dotenv_path = abspath(files.get_dotenv_path(paths.rootdir()), paths.rootdir())

        # get env variables from .env file
        if dotenv_path and os.path.isfile(dotenv_path):
            load_dotenv(dotenv_path)

        profiles = self.read(metadata_files)

        # empty profile if profile not found
        if profile_name not in self._info['profiles']:
            self.raiseException(f'Profile "{profile_name}" not found.')

        # read metadata, get the profile, if not found get an empty profile
        profiles = self.inherit(profiles)
        metadata = profiles[profile_name]

        # render any jinja templates in the profile
        md = self.render(metadata)

        # validate
        self.validate(md)

        # format
        md = self.formatted(md)

        # merge parameters from call
        if isinstance(parameters, dict):
            md['variables'] = merge(md['variables'], parameters)

        self._profile = YamlDict(md)
        self._info['active'] = profile_name

        return self

    def info(self):
        return YamlDict(self._info)

    def profile(self, section=None):
        return self._profile if section is None else self._profile.get(section, None)


def info():
    return Metadata().info()


def profile(section=None):
    return Metadata().profile(section)


def load(profile_name='default', metadata_files=None, dotenv_path=None, parameters=None):
    return Metadata().load(profile_name, metadata_files, dotenv_path, parameters)
