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

from datalabframework._utils import Singleton, YamlDict, repo_data, to_ordered_dict, python_version, get_relpath

from datalabframework._notebook import NotebookFinder

import uuid

class Project(metaclass=Singleton):

    def get_info(self):
        
        return {
                'dlf_version': __version__,
                'python_version': python_version(),
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
                    'dotenv': get_relpath(self._dotenv_path, paths.rootdir())
                }
        }

    def __init__(self, filename=None):
        #object variables
        self._profile = None
        self._metadata = {}
        self._dotenv_path = None
        self._engine = engine.NoEngine()
        self._info = self.get_info()

    def load(self, profile='default', rootdir_path=None, search_parent_dirs=True, factory_defaults=True):

        # init workdir and rootdir paths
        paths.set_rootdir(rootdir_path, search_parent_dirs)

        # set dotenv default file, check the file exists
        self._dotenv_path = files.get_dotenv_file(paths.rootdir())
        
        # metadata files
        metadata_files = files.get_metadata_files(paths.rootdir())

        # load metadata
        try:
            md_files = [ os.path.join(paths.rootdir(), x) for x in metadata_files]
            self._metadata = reader.load(profile,md_files,self._dotenv_path, factory_defaults)
        except ValueError as e:
            print(e)
            self._metadata = {}
 
        # bail if no metadata
        if not self._metadata:
            raise ValueError('No valid metadata to load.')
            
        # set profile from metadata
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
        logging.init(self._metadata, self._info['session_id'])
        
        return self

    def config(self):
        return YamlDict(self._info)

    def profile(self):
        return self._profile

    def metadata(self):
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
                    'kafka'
                    )
                )
                )
            )
        )
        
        if self._metadata.get('variables'):
            s = dict(sorted(self._metadata['variables'].items()))
            self._metadata['variables']= s
            
        return YamlDict(to_ordered_dict(self._metadata, keys))

    def resource(self, path=None, provider=None, md=dict()):
        if not self.profile:
            raise ValueError("No project profile loaded. Try first: datalabframework.project.load(...)") 

        md = resource.get_metadata(paths.rootdir(), self._metadata , path, provider, md)
        return md

    def engine(self):
        return self._engine
