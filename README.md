
# Datafaucet

Basic example and directory structure.

## Elements

This ETL/Data Science scaffolding works using a number of elements:

  - The introductory python notebook you are reading now (main.ipynb)
  - A directory structure for code and data processing (data)
  - The datafaucet python package (datafaucet)
  - Configuration files (metadata.yml, \__main__.py, Makefile)

## Principles ##

- ** Both notebooks and code are first citizens **

In the source directory `src` you will find all source code. In particular, both notebooks and code files are treated as source files. Source code is further partitioned and scaffolded in several directories to simplify and organize the data science project. Following python package conventions, the root of the project is tagged by a `__main__.py` file and directory contains the `__init__.py` code. By doing so, python and notebook files can reference each other.

Python notebooks and Python code can be mixed and matched, and are interoperable with each other. You can include function from a notebook to a python code, and you can include python files in a notebook. 

- ** Data Directories should not contain logic code **

Data can be located anywhere, on remote HDFS clusters, or Object Store Services exposed via S3 protocols etc. Also you can keep data on the local file system. For illustration purposes, this demo will use a local directory for data scaffolding. 

Separating data and code is done by moving all configuration to metadata files. Metadata files make possible to define aliases for data resources, data services and spark configurations, and keeping the ETL and ML code tidy with no hardcoded parameters.

- ** Decouple Code from Configuration **

Code either stored as notebooks or as python files should be decoupled from both engine configurations and from data locations. All configuration is kept in `metadata.yml` yaml files. Multiple setups for test, exploration, production can be described  in the same `metadata.yml` file or in separate multiple files using __profiles__. All profile inherit from a default profiles, to reduce dupllication of configurations settings across profiles.

- ** Declarative Configuration **

Metadata files are responsible for the binding of data and engine configurations to the code. For instance all data in the code shouold be referenced by an alias, and storage and retrieval of data object and files should happen via a common API. The metadata yaml file, describes the providers for each data source as well as the mapping of data aliases to their corresponding data objects. 



## Project Template

The data science project is structured in a way to facilitate the deployment of the artifacts, and to switch from batch processing to live experimentation. The top level project is composed of the following items:

### Top level Structure

```
├── binder
├── ci
├── data
├── resources
├── src
├── test
│
├── main.ipynb
├── versions.ipynb
│
├── __main__.py
├── metadata.yml
│
└── Makefile

```

## datafaucet


```python
import datafaucet as dfc
```

### Package things
Package version: package variables `version_info`, `__version__`


```python
dfc.version_info
```




    (0, 7, 1)




```python
dfc.__version__
```




    '0.7.1'



Check is the datafaucet is loaded in the current python context


```python
try:
    __DATALOOF__
    print("the datafaucet is loaded")
except NameError:
    print("the datafaucet is not loaded")
```

    the datafaucet is loaded



```python
#list of modules loaded as `from datafaucet import * ` 
dfc.__all__
```




    ['logging', 'project']



### Modules: project

Project is all about setting the correct working directories where to run and find your notebooks, python files and configuration files. When the datafaucet is imported, it starts by searching for a `__main__.py` file, according to python module file naming conventions. All modules and alias paths are all relative to this project root path.

#### Load a project profile

Loading the profile can be done with the `datafaucet.project.load` function call. It will look for files ending with `metadata.yml`. The function can optionally set the current working directory and import the key=values of .env file into the python os environment. if no parameters are specified, the default profile is loaded.


```python
help(dfc.project.load)
```

    Help on function load in module datafaucet.project:
    
    load(profile='default', rootdir_path=None, search_parent_dirs=True, dotenv=True, factory_defaults=True)
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
                    datafaucet:
                        name: dfc
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
        type `help(datafaucet.project.metadata)`
    



```python
dfc.project.load()
```

### Metadata profiles

#### Metadata files

     1) Metadata files are merged up, so you can split the information in multiple files as long as they end with `metadata.yml`
        For example: `metadata.yml`, `abc.metadata.yaml`, `abc_metadata.yml` are all valid metadata file names.
    
     2) All metadata files in all subdirectories from the project root directory are loaded,
        unless the directory contains a file `metadata.ignore.yml`
    
     3) Metadata files can provide multiple profile configurations, 
        by separating each profile configuration with a Document Marker ( a line with `---`) 
        (see https://yaml.org/spec/1.2/spec.html#YAML)
     
     4) Each metadata profile, can be broken down in multiple yaml files,
        When loading the files all configuration belonging to the same profile with be merged. 
     
     5) All metadata profiles inherit the settings from profile 'default'
     
     6) An empty metadata profile inherits from a factory default set as:
         """
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
                datafaucet:
                    name: dfc
                    stream:
                        enable: true
                        severity: notice
         """
    

     Metadata files are composed of 6 sections:
         - profile 
         - variables
         - providers
         - resources
         - engine
         - loggers

 - jinja templates for variable substitution 
 - environment variables as jinja template function __env('MY_ENV_VARIABLE', my_default_value)__
 - current timestamp as a jinja template function __now(timezone='UTC', format='%Y-%m-%d %H:%M:%S')__
 - multiple profiles, inheriting from the __default__ profile


```python
md = dfc.project.metadata()
md
```




    profile: default
    variables:
        my_concat_var: hello spark running at (local[*])
        my_env_var: guest
        my_nested_var: 'hello spark running at (local[*]): the current date is 2019-03-25'
        my_date_var: '2019-03-25'
        my_string_var: hello
    engine:
        type: spark
        master: local[*]
    providers:
        localfs:
            service: file
            format: csv
            path: data
    resources:
        ascombe:
            path: ascombe.csv
            provider: localfs
        correlation:
            path: correlation.csv
            provider: localfs
    loggers:
        root:
            severity: info
        datafaucet:
            name: dfc
            stream:
                enable: true
                severity: notice
            kafka:
                enable: false
                severity: info
                topic: dfc
                hosts: kafka-node1:9092 kafka-node2:9092



## Inspect current project configuration

You can inspect the current project configuration, by calling the `datafaucet.project.config` function.


```python
help(dfc.project.config)
```

    Help on function config in module datafaucet.project:
    
    config()
        Returns the current project configuration
        :return: a dictionary with project configuration data
    


#### Project configuration

The current loaded project configuration can be inspected with `datafaucet.project.config()` function call. 
The following information is available in the returned dictionary:

| key                      | explanation                                                                                 | example value                                     |
| :---                     | :----                                                                                            |--------------------------------------------------:|
| python_version           | version of python running the current script                                                | 3.6.7                                             |
| session_id               | session unique for this particular script run                                               | 0xf3df202e4c6f11e9                                |
| profile                  | name of the metadata profile loaded                                                         | default                                           |
| filename                 | name of the current script (works both for .py and ipynb files )                            | main.ipynb                                        |
| rootdir                  | The root directory of the project (marked by an empty __main__.py or __main__.ipynb file)   | /home/jovyan/work/basic                           |
| workdir                  | The current working directory                                                               | /home/jovyan/work/basic                           |
| username                 | User running the script                                                                     | jovyan                                            |
| repository               | Information about the current git repository (if available)                                 |                                                   |
| repository.type          | The type of revision system (supports only git currently)                                   | git                                               |
| repository.committer     | Last committer on this repository                                                           | Natalino Busa                                     |
| repository.hash          | last commit short hash (only 7 chars)                                                       | 5e43848                                           |
| repository.commit        | Last committer full hash                                                                    | 5e4384853398941f4b52cb4102145ee98bdeafa6          |
| repository.branch        | repo branch name                                                                            | master                                            |
| repository.url           | url of the repository                                                                       | https://github.com/natbusa/datafaucet.git   |
| repository.name          | repository name                                                                             | datafaucet.git                              |
| repository.date          | Date of last commit                                                                         | 2019-03-22T04:21:07+00:00                         |
| repository.clean         | Repository does not contained modified files, wrt to commited data                          | False                                             |
| files                    | python, notebooks and configuration files in this project                                   |                                                   |
| files.notebooks          | notebooks files (*.ipynb) in all subdirectories starting from rootdir                       | main.ipynb                                        |
| files.python             | notebooks files (*.py) in all subdirectories starting from rootdir                          | __main__.py                                       |
| files.metadata           | metadata files (*.yml) in all subdirectories starting from rootdir                          | metadata.yml                                      |
| files.dotenv             | filename with variables definitions, unix style                                             | .env                                              |
| engine                   | data engine configuration                                                                   |                                                   |
| engine.type              | data engine type                                                                            | spark                                             |
| engine.name              | name (generated using git repo name and metadata profile)                                   | default                                           |
| engine.version           | engine version                                                                              | 2.4.0                                             |
| engine.config            | engine configuration (key-values list)                                                      | spark.master: local[*]                            |
| engine.env               | engine environment variables (key-values list)                                              | PYSPARK_SUBMIT_ARGS: ' pyspark-shell'             |
| engine.rootdir           | engine rootdir (same as above)                                                              | /home/jovyan/work/basic                           |
| engine.timezone          | engine timezone configuration                                                               | UTC                                               |



```python
dfc.project.config()
```




    dfc_version: 0.7.1
    python_version: 3.6.7
    session_id: '0x8aa0374e4ee811e9'
    profile: default
    filename: main.ipynb
    rootdir: /home/jovyan/work/basic
    workdir: /home/jovyan/work/basic
    username: jovyan
    repository:
        type:
        committer: ''
        hash: 0
        commit: 0
        branch: ''
        url: ''
        name: ''
        date: ''
        clean: false
    files:
        notebooks:
          - main.ipynb
          - versions.ipynb
          - hello.ipynb
        python:
          - __main__.py
        metadata:
          - metadata.yml
        dotenv: .env
    engine:
        type: spark
        name: default
        version: 2.4.0
        config:
            spark.driver.port: '46739'
            spark.rdd.compress: 'True'
            spark.app.name: default
            spark.serializer.objectStreamReset: '100'
            spark.master: local[*]
            spark.executor.id: driver
            spark.submit.deployMode: client
            spark.app.id: local-1553509627579
            spark.ui.showConsoleProgress: 'true'
            spark.driver.host: 36594ccded11
        env:
            PYSPARK_SUBMIT_ARGS: ' pyspark-shell'
        rootdir: /home/jovyan/work/basic
        timezone:



Data resources are relative to the `rootpath`. 

### Resources

Data binding works with the metadata files. It's a good practice to declare the actual binding in the metadata and avoiding hardcoding the paths in the notebooks and python source files.


```python
md =dfc.project.resource('ascombe')
md
```




    {'hash': '0x80a539b9fc17d1c4',
     'url': '/home/jovyan/work/basic/data/ascombe.csv',
     'service': 'file',
     'format': 'csv',
     'host': '127.0.0.1',
     'port': None,
     'driver': None,
     'database': None,
     'username': None,
     'password': None,
     'resource_path': 'ascombe.csv',
     'provider_path': '/home/jovyan/work/basic/data',
     'provider_alias': 'localfs',
     'resource_alias': 'ascombe',
     'cache': None,
     'date_column': None,
     'date_start': None,
     'date_end': None,
     'date_window': None,
     'date_partition': None,
     'update_column': None,
     'hash_column': None,
     'state_column': None,
     'options': {},
     'mapping': {}}



### Modules: Engines

This submodules will allow you to start a context, from the configuration described in the metadata. It also provide, basic load/store data functions according to the aliases defined in the configuration.

Let's start by listing the aliases and the configuration of the engines declared in `metadata.yml`.


__Context: Spark__  
Let's start the engine session, by selecting a spark context from the list. Your can have many spark contexts declared, for instance for single node 


```python
import datafaucet as dfc
engine = dfc.project.engine()
engine.config()
```




    type: spark
    name: default
    version: 2.4.0
    config:
        spark.driver.port: '46739'
        spark.rdd.compress: 'True'
        spark.app.name: default
        spark.serializer.objectStreamReset: '100'
        spark.master: local[*]
        spark.executor.id: driver
        spark.submit.deployMode: client
        spark.app.id: local-1553509627579
        spark.ui.showConsoleProgress: 'true'
        spark.driver.host: 36594ccded11
    env:
        PYSPARK_SUBMIT_ARGS: ' pyspark-shell'
    rootdir: /home/jovyan/work/basic
    timezone:



You can quickly inspect the properties of the context by calling the `info()` function

By calling the `context` method, you access the Spark SQL Context directly. The rest of your spark python code is not affected by the initialization of your session with the datafaucet.


```python
engine = dfc.project.engine()
spark = engine.context()
```

Once again, let's read the csv data again, this time using the spark context. First using the engine `write` utility, then directly using the spark context and the `dfc.data.path` function to localize our labeled dataset.


```python
#read using the engine utility (directly using the load function)
df = engine.load('ascombe', header=True, inferSchema=True)
```


```python
#read using the engine utility (also from resource metadata)
md =dfc.project.resource('ascombe')
df = engine.load(md, header=True, inferSchema=True)
```


```python
df.printSchema()
```

    root
     |-- idx: integer (nullable = true)
     |-- Ix: double (nullable = true)
     |-- Iy: double (nullable = true)
     |-- IIx: double (nullable = true)
     |-- IIy: double (nullable = true)
     |-- IIIx: double (nullable = true)
     |-- IIIy: double (nullable = true)
     |-- IVx: double (nullable = true)
     |-- IVy: double (nullable = true)
    



```python
df.show()
```

    +---+----+-----+----+----+----+-----+----+----+
    |idx|  Ix|   Iy| IIx| IIy|IIIx| IIIy| IVx| IVy|
    +---+----+-----+----+----+----+-----+----+----+
    |  0|10.0| 8.04|10.0|9.14|10.0| 7.46| 8.0|6.58|
    |  1| 8.0| 6.95| 8.0|8.14| 8.0| 6.77| 8.0|5.76|
    |  2|13.0| 7.58|13.0|8.74|13.0|12.74| 8.0|7.71|
    |  3| 9.0| 8.81| 9.0|8.77| 9.0| 7.11| 8.0|8.84|
    |  4|11.0| 8.33|11.0|9.26|11.0| 7.81| 8.0|8.47|
    |  5|14.0| 9.96|14.0| 8.1|14.0| 8.84| 8.0|7.04|
    |  6| 6.0| 7.24| 6.0|6.13| 6.0| 6.08| 8.0|5.25|
    |  7| 4.0| 4.26| 4.0| 3.1| 4.0| 5.39|19.0|12.5|
    |  8|12.0|10.84|12.0|9.13|12.0| 8.15| 8.0|5.56|
    |  9| 7.0| 4.82| 7.0|7.26| 7.0| 6.42| 8.0|7.91|
    | 10| 5.0| 5.68| 5.0|4.74| 5.0| 5.73| 8.0|6.89|
    +---+----+-----+----+----+----+-----+----+----+
    


Finally, let's calculate the correlation for each set I,II, III, IV between the `x` and `y` columns and save the result on an separate dataset.


```python
from pyspark.ml.feature import VectorAssembler

for s in ['I', 'II', 'III', 'IV']:
    va = VectorAssembler(inputCols=[s+'x', s+'y'], outputCol=s)
    df = va.transform(df)
    df = df.drop(s+'x', s+'y')
    
df.show()
```

    +---+------------+-----------+------------+-----------+
    |idx|           I|         II|         III|         IV|
    +---+------------+-----------+------------+-----------+
    |  0| [10.0,8.04]|[10.0,9.14]| [10.0,7.46]| [8.0,6.58]|
    |  1|  [8.0,6.95]| [8.0,8.14]|  [8.0,6.77]| [8.0,5.76]|
    |  2| [13.0,7.58]|[13.0,8.74]|[13.0,12.74]| [8.0,7.71]|
    |  3|  [9.0,8.81]| [9.0,8.77]|  [9.0,7.11]| [8.0,8.84]|
    |  4| [11.0,8.33]|[11.0,9.26]| [11.0,7.81]| [8.0,8.47]|
    |  5| [14.0,9.96]| [14.0,8.1]| [14.0,8.84]| [8.0,7.04]|
    |  6|  [6.0,7.24]| [6.0,6.13]|  [6.0,6.08]| [8.0,5.25]|
    |  7|  [4.0,4.26]|  [4.0,3.1]|  [4.0,5.39]|[19.0,12.5]|
    |  8|[12.0,10.84]|[12.0,9.13]| [12.0,8.15]| [8.0,5.56]|
    |  9|  [7.0,4.82]| [7.0,7.26]|  [7.0,6.42]| [8.0,7.91]|
    | 10|  [5.0,5.68]| [5.0,4.74]|  [5.0,5.73]| [8.0,6.89]|
    +---+------------+-----------+------------+-----------+
    


After assembling the dataframe into four sets of 2D vectors, let's calculate the pearson correlation for each set. In the case the the ascombe sets, all sets should have the same pearson correlation.


```python
from pyspark.ml.stat import Correlation
from pyspark.sql.types import DoubleType

corr = {}
cols = ['I', 'II', 'III', 'IV']

# calculate pearson correlations
for s in cols:
    corr[s] = Correlation.corr(df, s, 'pearson').collect()[0][0][0,1].item()

# declare schema
from pyspark.sql.types import StructType, StructField, FloatType
schema = StructType([StructField(s, FloatType(), True) for s in cols])

# create output dataframe
corr_df = spark.createDataFrame(data=[corr], schema=schema)
```


```python
import pyspark.sql.functions as f
corr_df.select([f.round(f.avg(c), 3).alias(c) for c in cols]).show()
```

    +-----+-----+-----+-----+
    |    I|   II|  III|   IV|
    +-----+-----+-----+-----+
    |0.816|0.816|0.816|0.817|
    +-----+-----+-----+-----+
    


Save the results. It's a very small data frame, however Spark when saving  csv format files, assumes large data sets and partitions the files inside an object (a directory) with the name of the target file. See below:



```python
engine.save(corr_df,'correlation')
```




    True



We read it back to chack all went fine


```python
engine.load('correlation', header=True, inferSchema=True).show()
```

    +---+---------+---------+----------+----------+
    |_c0|        I|       II|       III|        IV|
    +---+---------+---------+----------+----------+
    |  0|0.8164205|0.8162365|0.81628674|0.81652147|
    +---+---------+---------+----------+----------+
    


### Modules: Export

This submodules will allow you to export cells and import them in other notebooks as python packages. Check the notebook [versions.ipynb](versions.ipynb), where you will see how to export the notebook, then follow the code here below to check it really works!



```python
import datafaucet as dfc
dfc.project.load()

from hello import python_version
```

    importing Jupyter notebook from hello.ipynb



```python
python_version()
```

    Hello world: python 3.6.7
