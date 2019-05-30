import os
import sys
import getpass

from datalabframework import __version__

from datalabframework import paths
from datalabframework import files
from datalabframework import logging

# engine
from datalabframework import engine as dlf_engine

# metadata
from datalabframework import metadata
from datalabframework.resource import metadata as get_resource_metadata

#utils
from datalabframework.yaml import YamlDict
from datalabframework._utils import Singleton, repo_data, to_ordered_dict, python_version, relpath, abspath

import uuid

class Project(metaclass=Singleton):

    def __init__(self):
        #object variables
        self._profile = None
        self._metadata = {}
        self._engine = None
        self._loaded = False
        self._session_id = 0
        self._username = None
        self._repo = {}
        
        self._filepath = None
        self._dotenv_path = None
        self._metadata_files = {}
        self._notebook_files = {}
        self._python_files = {}
        
        self._no_reload = False

    def load(self, profile='default', rootpath=None):
        """
        Performs the following steps:
            - set rootdir for the given project
            - import variables from  <rootdir>/.env (if present),
            - load the `profile` from the metadata files
            - setup and start the data engine

        :param profile: load the given metadata profile (default: 'default')
        
        :param rootpath: root directory for loaded project 
               default behaviour: search parent dirs to detect rootdir by 
               looking for a '__main__.py' or 'main.ipynb' file. 
               When such a file is found, the corresponding directory is the 
               root path for the project. If nothing is found, the current 
               working directory, will be the rootpath

        :return: None

        Notes abount metadata configuration:

        1)  Metadata files are merged up, so you can split the information in 
            multiple files as long as they end with `metadata.yml`. 

            For example: `metadata.yml`, `abc.metadata.yaml`, `abc_metadata.yml` 
            are all valid metadata file names.

        2)  All metadata files in all subdirectories from the project root directory 
            are loaded, unless the directory contains a file `metadata.ignore.yml`

        3)  Metadata files can provide multiple profile configurations,
            by separating each profile configuration with a Document Marker 
            ( a line with `---`) (see https://yaml.org/spec/1.2/spec.html#YAML)

        4)  Each metadata profile, can be broken down in multiple yaml files,
            When loading the files all configuration belonging to the same profile 
            with be merged.

        5)  All metadata profiles inherit the settings from profile 'default'

        Metadata files are composed of 6 sections:
            - profile
            - variables
            - providers
            - resources
            - engine
            - loggers

        For more information about metadata configuration,
        type `help(datalabframework.project.metadata)`    
        """
        
        if self._loaded and self._no_reload:
            logging.notice(
                f"Profile {self._profile} already loaded. " 
                 "Skipping project.load()")
            return self
        
        # set rootpath
        paths.set_rootdir(rootpath)

        # set loaded to false
        self._loaded = False
        
        # set session id
        self._session_id  = hex(uuid.uuid1().int>>64)
        
        # set username
        self._username = getpass.getuser()
        
        # get repo data
        self._repo = repo_data()

        # get currently running script path
        self._script_path = files.get_script_path(paths.rootdir())
        
        # set dotenv default file, check the file exists
        self._dotenv_path = files.get_dotenv_path(paths.rootdir())
        
        # get files
        self._metadata_files = files.get_metadata_files(paths.rootdir())
        self._notebook_files = files.get_jupyter_notebook_files(paths.rootdir())
        self._python_files = files.get_python_files(paths.rootdir())

        # metadata defaults
        dir_path = os.path.dirname(os.path.realpath(__file__))
        default_md_files = [os.path.join(dir_path, 'schemas/default.yml')]
        project_md_files = abspath(self._metadata_files, paths.rootdir())

        # load metadata
        try:
            md_paths = default_md_files + project_md_files
            dotenv_path = abspath(self._dotenv_path, paths.rootdir())
            
            metadata.load(profile,md_paths,dotenv_path)
        except ValueError as e:
            print(e)
            
        # bail if no metadata
        if metadata.profile is None:
            raise ValueError('No valid metadata to load.')
            
        # set profile from metadata
        self._profile_name = metadata.get_loaded_profile_name()

        # add roothpath to the list of python sys paths
        if paths.rootdir() not in sys.path:
            sys.path.append(paths.rootdir())
        
        # stop existing engine
        if self._engine:
            self._engine.stop()

        # craft the engine name
        L = [self._profile, self._repo.get('name')]
        name = '-'.join([x for x in L if x])

        #initialize the engine
        self._engine = dlf_engine.get(name, self._metadata, paths.rootdir())

        # initialize logging
        logging.init(
            self._metadata.get('loggers'), 
            self._session_id,
            self._username,
            self._script_path,
            self._repo['name'],
            self._repo['hash']
        )
        
        # set loaded to True
        self._loaded = True
        
        # return object
        return self

    def check_if_loaded(self):
        if not self._loaded :
            raise ValueError(
                "No project profile loaded. " +
                "Execute datalabframework.project.load(...) first.")
        return

    def config(self):
        self.check_if_loaded()
        
        return YamlDict({
            'version': __version__,
            'username': self._username,
            'session_id': self._session_id,
            'profile': self._profile,
            'rootdir': paths.rootdir(),
            'script_path': self._script_path,
            'dotenv_path': self._dotenv_path,
            'notebooks_files': self._notebook_files,
            'python_files': self._python_files,
            'metadata_files': self._metadata_files,
            'repository': self._repo
        })

    @property
    def profile(self):
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
            - loggers

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
        reuse them in other part of the configuration.  The datalabframework yaml files 
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

        A provider is a service which allows you to load and save data. The datalabframework 
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
        
        
        self.check_if_loaded()
        return metadata.profile

    def resource(self, path=None, provider=None, md=None, **kargs):
        self.check_if_loaded()
        md = get_resource_metadata(
            paths.rootdir(), 
            self._metadata , 
            path, 
            provider, 
            md, 
            **kargs)
        return md

    def engine(self):
        self.check_if_loaded()
        return self._engine

def load(profile='default', rootpath=None):
    return Project().load(profile, rootpath)

def config():
    return Project().config()

def engine():
    return Project().engine()

def profile():
    return Project().profile()

def resource(path=None, provider=None, md=None, **kargs):
    """
    returns a resource object for read and write operations
    This object provides a config() method which returns the dictionary

    :param path: the path or the alias of the resource
    :param provider: as defined in the loaded metadata profile
    :param md: dictionary override
    :return: a resouce object
    """
    return Project().resource(path, provider, md, **kargs)

# doc methods from class methods
load.__doc__ = Project.load.__doc__
config.__doc__ = Project.config.__doc__
engine.__doc__ = Project.engine.__doc__
profile.__doc__ = Project.profile.__doc__

