import os
import sys
import getpass
import uuid

from datafaucet import __version__

from datafaucet import paths
from datafaucet import files
from datafaucet import git
from datafaucet import logging

# engine
from datafaucet import engines

# metadata
from datafaucet import metadata
from datafaucet.resources import Resource

# utils
from datafaucet.yaml import YamlDict
from datafaucet.utils import Singleton, abspath


class Project(metaclass=Singleton):

    def __init__(self):
        # object variables
        self._profile = None
        self._metadata = {}
        self._engine = None
        self._session_id = 0
        self._username = None
        self._repo = {}

        self._filepath = None
        self._dotenv_path = None
        self._metadata_files = {}
        self._notebook_files = {}
        self._python_files = {}

        self.loaded = False
        self._reload = True

        self._script_path = None
        self._session_name = None
        self._profile_name = None

    def load(self, profile='default', rootpath=None, reload=True, parameters=None):
        """
        Performs the following steps:
            - set rootpath for the given project
            - import variables from  <rootpath>/.env (if present),
            - load the `profile` from the metadata files
            - setup and start the data engine

        :param profile: load the given metadata profile (default: 'default')
        
        :param rootpath: root directory for loaded project 
               default behaviour: search parent dirs to detect rootdir by 
               looking for a '__main__.py' or 'main.ipynb' file. 
               When such a file is found, the corresponding directory is the 
               root path for the project. If nothing is found, the current 
               working directory, will be the rootpath

        :param reload: if set to false, it prevents to reload a new profile
        :param parameters: optional dict, available as metadata variables

        :return: None

        Notes about metadata configuration:

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
            - logging

        For more information about metadata configuration,
        type `help(datafaucet.project.metadata)`    
        """

        if self.loaded and not self._reload:
            logging.notice(
                f"Profile '{self._profile}' already loaded  with option 'reload=False':"
                "Skipping project.load()")
            return self

        # set rootpath
        paths.set_rootdir(rootpath)

        # set loaded to false
        self.loaded = False

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

            metadata.load(profile, md_paths, dotenv_path, parameters=parameters)
        except ValueError as e:
            print(e)

        # bail if no metadata
        if metadata.profile is None:
            raise ValueError('No valid metadata to load.')

        # set username
        self._username = getpass.getuser()

        # get repo data
        self._repo = git.repo_data()

        # set profile from metadata
        self._profile = metadata.info()['active']

        # set session name
        name_parts = [self._profile, self._repo.get('name')]
        self._session_name = '-'.join([x for x in name_parts if x])

        # set session id
        self._session_id = hex(uuid.uuid1().int >> 64)

        # initialize logging
        log_config = metadata.profile('logging')
        logging.init(
            log_config['level'],
            log_config['stdout'],
            log_config['file'],
            log_config['kafka'],
            self._session_id
        )

        # add rootpath to the list of python sys paths
        if paths.rootdir() not in sys.path:
            sys.path.append(paths.rootdir())

        # stop existing engine
        if self._engine:
            self._engine.stop()

        # services
        services = dict()

        all_aliases = list(metadata.profile()['providers'].keys())

        # get services from aliases
        for alias in all_aliases:
            r = Resource(alias)
            services[r['service']] = r

        # get one service from each type to 
        # load drivers, jars etc via the engine init
        services = list(services.values())

        # initialize the engine
        md = metadata.profile()['engine']
        
        engines.Engine(
            md['type'],
            session_name=self._session_name,
            session_id=self._session_id,
            master=md['master'],
            timezone=md['timezone'],
            jars=md['jars'],
            packages=md['packages'],
            files=md['files'],
            repositories=md['repositories'],
            services=services,
            conf=md['conf'],
            detect=md['detect']
        )

        # set loaded to True
        self.loaded = True
        self._reload = reload

        # return object
        return self

    def info(self):
        if not self.loaded:
            logging.error(
                "No project profile loaded. " +
                "Execute datafaucet.project.load(...) first.")
            return None

        return YamlDict({
            'version': __version__,
            'username': self._username,
            'session_name': self._session_name,
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


def info():
    return Project().info()


def load(profile='default', rootpath=None, reload=True, parameters=None):
    return Project().load(profile, rootpath, reload, parameters)
