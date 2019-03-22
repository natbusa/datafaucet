from datalabframework._project import Project

def load(profile='default', rootdir_path=None, search_parent_dirs=True, dotenv_path=None):

    """
    Performs the following steps:
      - set rootdir for the given project
      - perform .env env variable exporting,
      - load the given `profile` from the metadata files,
      - setup and start the data engine

     Note that:
     1) Metadata files are merged up, so you can split the information in multiple files as long as they end with metadata.yml
        metadata.yml, abc.metadata.yaml, abc_metadata.yml are all valid metadata file names.

     2) All metadata files in all subdirectories from the project root directory are loaded,
        unless the directory contains a file `metadata.ignore.yml`

     3) all metadata profiles inherit the settings from profile 'default'

    :param profile: load the given metadata profile (default: 'default')
    :param rootdir_path: root directory for loaded project (default: current working directory)
    :param search_parent_dirs: search parent dirs to detect rootdir by looking for a '__main__.py' or 'main.ipynb' file (default: True)
    :param dotenv_path: load variable from a dotenv file, if the file exists and readable (default: <rootdir>/.env)
    :return: None
    """
    Project().load(profile, rootdir_path, search_parent_dirs, dotenv_path)
    return

def config():
    """
    Returns the current project configuration
    :return: a dictionary with project configuration data
    """
    return Project().config()

def engine():
    """
    Get the engine defined in the loaded metadata profile
    :return: the Engine object
    """
    return Project().engine()

def metadata():
    """
    return a metadata object which provides just one method:
    :return: a Metadata object
    """
    return Project().metadata()

def resource(path=None, provider=None, md=dict()):
    """
    returns a resource object for read and write operations
    This object provides a config() method which returns the dictionary

    :param path: the path or the alias of the resource
    :param provider: as defined in the current metadata profile
    :param md: dictionary override
    :return: a Resouce Object
    """
    return Project().resource(path, provider, md)
