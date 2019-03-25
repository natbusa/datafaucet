from datalabframework._project import Project

def load(profile='default', rootdir_path=None, search_parent_dirs=True, dotenv=True, factory_defaults=True):

    """
    Performs the following steps:
        - set rootdir for the given project
        - perform .env env variable exporting,
        - load the given `profile` from the metadata files,
        - setup and start the data engine

    :param profile: load the given metadata profile (default: 'default')
    :param rootdir_path: root directory for loaded project (default: current working directory)
    :param search_parent_dirs: search parent dirs to detect rootdir by looking for a '__main__.py' or 'main.ipynb' file (default: True)
    :param factory_defaults: add preset default configuration. project provided metadata file can override this default values (default: True)
    :param dotenv: load variable from a dotenv file, if the file exists and readable (default 'True' looks for the file <rootdir>/.env)
    :return: None

    Note that:

    1)  Metadata files are merged up, so you can split the information in multiple files as long as they end with `metadata.yml`
        For example: `metadata.yml`, `abc.metadata.yaml`, `abc_metadata.yml` are all valid metadata file names.

    2)  All metadata files in all subdirectories from the project root directory are loaded,
        unless the directory contains a file `metadata.ignore.yml`

    3)  Metadata files can provide multiple profile configurations,
        by separating each profile configuration with a Document Marker ( a line with `---`)
        (see https://yaml.org/spec/1.2/spec.html#YAML)

    4)  Each metadata profile, can be broken down in multiple yaml files,
        When loading the files all configuration belonging to the same profile with be merged.

    5)  All metadata profiles inherit the settings from profile 'default'

    6)  If `factory_defaults` is set to true, 
        the provided metadata profiles will inherits from a factory defaults file set as:
         ```
            %YAML 1.2
            ---
            profile: default
            variables: {}
            engine:
                type: spark
                master: local[*]
            providers: {}
            resources: {}
            loggers:
                root:
                    severity: info
                datalabframework:
                    name: dlf
                    stream:
                        enable: true
                        severity: notice
            ---
            profile: prod
            ---
            profile: stage
            ---
            profile: test
            ---
            profile: dev

         ```

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
    Project().load(profile, rootdir_path, search_parent_dirs, dotenv, factory_defaults)
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
