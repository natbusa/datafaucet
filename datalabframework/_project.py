import os
import sys
import getpass

from datalabframework import __version__

from datalabframework import paths
from datalabframework import files
from datalabframework import engine
from datalabframework import logging

from datalabframework.metadata import reader
from datalabframework.metadata import resource

from datalabframework._utils import Singleton, YamlDict, repo_data
from datalabframework._notebook import NotebookFinder

import uuid

class Project(metaclass=Singleton):

    def get_info(self):
        return {
                'dlf_version': __version__,
                'python_version': '.'.join([str(x) for x in sys.version_info[0:3]]),
                'session_id': hex(uuid.uuid1().int>>64),
                'profile': self._profile,
                'filename': os.path.relpath(files.get_current_filename(), paths.rootdir()),
                'rootdir': paths.rootdir(),
                'workdir': paths.workdir(),
                'username': getpass.getuser(),
                'repository': repo_data(),
                'files': {
                    'notebooks': files.get_jupyter_notebook_files(paths.rootdir()),
                    'python': files.get_python_files(paths.rootdir()),
                    'metadata': files.get_metadata_files(paths.rootdir()),
                    'dotenv': os.path.relpath(self._dotenv_path, paths.rootdir()) if self._dotenv_path else None,
                },
                'engine': self._engine.config().to_dict()
        }

    def __init__(self, filename=None):
        #object variables
        self._profile = None
        self._metadata = reader.default_metadata
        self._dotenv_path = None
        self._engine = engine.NoEngine()
        self._info = self.get_info()

    def load(self, profile='default', rootdir_path=None, search_parent_dirs=True, dotenv_path=None):

        # init workdir and rootdir paths
        paths.set_rootdir(rootdir_path, search_parent_dirs)

        # set dotenv default file
        if dotenv_path is None:
            dotenv_path = os.path.join(paths.rootdir(), '.env')

        # if file is valid and exists, store in the object context.
        if os.path.isfile(dotenv_path):
            self._dotenv_path = os.path.abspath(dotenv_path)

        # metadata files
        metadata_files = files.get_metadata_files(paths.rootdir())

        # load metadata
        md = reader.load(
            profile,
            [ os.path.join(paths.rootdir(), x) for x in metadata_files],
            self._dotenv_path)

        # validate metadata
        reader.validate(md)

        # store metadata in project object
        self._metadata = md

        # set profile, only if not set yet
        self._profile = self._metadata['profile']

        # add roothpath to the list of python sys paths
        if paths.rootdir() not in sys.path:
            sys.path.append(paths.rootdir())

        # register hook for loading ipynb files
        if 'NotebookFinder' not in str(sys.meta_path):
            sys.meta_path.append(NotebookFinder())

        # initialize the engine
        repo_name = repo_data()['name']
        jobname = '{}-{}'.format(self._profile, repo_name) if repo_name else self._profile

        # stop existing engine
        if self._engine:
            self._engine.stop()

        self._engine = engine.get(jobname, self._metadata, paths.rootdir())

        # get all project info
        self._info = self.get_info()

        # initialize logging
        logging.init(md, self._info['session_id'])
        
        return self

    def config(self):
        return YamlDict(self._info)

    def profile(self):
        return self._profile

    def metadata(self):
        return YamlDict(self._metadata)

    def resource(self, path=None, provider=None, md=dict()):
        if not self.profile:
            raise ValueError("No project profile loaded. Try first: datalabframework.project.load(...)") 

        md = resource.get_metadata(paths.rootdir(), self._metadata , path, provider, md)
        return md

    def engine(self):
        return self._engine
