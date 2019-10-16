import sys
import os, time
import shutil
import textwrap

import logging as python_logging

from datafaucet import logging
from datafaucet import elastic

from datafaucet.resources import Resource, get_local

from datafaucet.yaml import YamlDict, to_dict

from datafaucet._utils import python_version, get_hadoop_version_from_system
from datafaucet._utils import get_tool_home, run_command, str_join, merge

import pandas as pd
from datafaucet.spark import dataframe

from datafaucet.engines import EngineSingleton, EngineBase

from timeit import default_timer as timer

import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.utils import AnalysisException

# purpose of engines
# abstract engine init, data read and data write
# and move this information to metadata

# it does not make the code fully engine agnostic though.

import pyspark

class SparkEngine(EngineBase, metaclass=EngineSingleton):

    @staticmethod
    def set_conf_timezone(conf, timezone=None):
        assert(type(conf) == pyspark.conf.SparkConf)

        # if timezone set to 'naive',
        # force UTC to override local system and spark defaults
        # This will effectively avoid any conversion of datetime object to/from spark

        if timezone == 'naive':
            timezone = 'UTC'

        if timezone:
            os.environ['TZ'] = timezone
            time.tzset()
            conf.set('spark.sql.session.timeZone', timezone)
            conf.set('spark.driver.extraJavaOptions', f'-Duser.timezone={timezone}')
            conf.set('spark.executor.extraJavaOptions', f'-Duser.timezone={timezone}')
        else:
            # use spark and system defaults
            pass

    def set_info(self):
        hadoop_version = None
        hadoop_detect_from = None
        try:
            spark_session = pyspark.sql.SparkSession.builder.getOrCreate()
            hadoop_version = spark_session.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
            hadoop_detect_from = 'spark'
        except Exception as e:
            pass

        self._stop(spark_session)

        if hadoop_version is None:
            hadoop_version = get_hadoop_version_from_system()
            hadoop_detect_from = 'system'

        if hadoop_version is None:
            logging.warning('Could not find a valid hadoop install.')

        hadoop_home = get_tool_home('hadoop', 'HADOOP_HOME', 'bin')[0]
        spark_home = get_tool_home('spark-submit', 'SPARK_HOME', 'bin')[0]

        spark_dist_classpath = os.environ.get('SPARK_DIST_CLASSPATH')
        spark_dist_classpath_source = 'env'

        if not spark_dist_classpath:
            spark_dist_classpath_source = os.path.join(spark_home, 'conf/spark-env.sh')
            if os.path.isfile(spark_dist_classpath_source):
                with open(spark_dist_classpath_source) as s:
                    for line in s:
                        pattern = 'SPARK_DIST_CLASSPATH='
                        pos = line.find(pattern)
                        if pos >= 0:
                            spark_dist_classpath = line[pos + len(pattern):].strip()
                            spark_dist_classpath = run_command(f'echo {spark_dist_classpath}')[0]

        if hadoop_detect_from == 'system' and (not spark_dist_classpath):
            logging.warning(textwrap.dedent("""
                        SPARK_DIST_CLASSPATH not defined and spark installed without hadoop
                        define SPARK_DIST_CLASSPATH in $SPARK_HOME/conf/spark-env.sh as follows:

                           export SPARK_DIST_CLASSPATH=$(hadoop classpath)

                        for more info refer to:
                        https://spark.apache.org/docs/latest/hadoop-provided.html
                    """))

        self.info['python_version'] = python_version()
        self.info['hadoop_version'] = hadoop_version
        self.info['hadoop_detect'] = hadoop_detect_from
        self.info['hadoop_home'] = hadoop_home
        self.info['spark_home'] = spark_home
        self.info['spark_classpath'] = spark_dist_classpath.split(':') if spark_dist_classpath else None
        self.info['spark_classpath_source'] = spark_dist_classpath_source

        return

    def detect_submit_params(self, services=None):
        assert (isinstance(services, (type(None), str, list, set)))
        services = [services] if isinstance(services,str) else services
        services = services or []

        # if service is a string, make a resource out of it

        resources = [s if isinstance(s, dict) else Resource(service=s) for s in services ]

        # create a dictionary of services and versions,
        services = {}
        for r in resources:
            services[r['service']] = r['version']

        submit_types = ['jars', 'packages', 'repositories', 'py-files', 'files', 'conf']

        submit_objs = dict()
        for submit_type in submit_types:
            submit_objs[submit_type] = []

        if not services:
            return submit_objs

        services = dict(sorted(services.items()))

        # get hadoop, and configured metadata services
        hadoop_version = self.info['hadoop_version']

        #### submit: repositories
        repositories = submit_objs['repositories']

        #### submit: jars
        jars = submit_objs['jars']

        #### submit: packages
        packages = submit_objs['packages']

        for s, v in services.items():
            if s == 'mysql':
                packages.append(f'mysql:mysql-connector-java:{v}')
            elif s == 'sqlite':
                packages.append(f'org.xerial:sqlite-jdbc:{v}')
            elif s == 'postgres':
                packages.append(f'org.postgresql:postgresql:{v}')
            elif s == 'oracle':
                packages.append(f'com.oracle.ojdbc:ojdbc8:{v}')
            elif s == 'mssql':
                packages.append(f'com.microsoft.sqlserver:mssql-jdbc:{v}')
            elif s == 'clickhouse':
                packages.append(f'ru.yandex.clickhouse:clickhouse-jdbc:{v}')
            elif s == 's3a':
                if hadoop_version:
                    packages.append(f"org.apache.hadoop:hadoop-aws:{hadoop_version}")
                else:
                    logging.warning('The Hadoop installation is not detected. '
                                    'Could not load hadoop-aws (s3a) package ')
            else:
                logging.warning(f'could not autodetect driver to install for {s}, version {v}')

        #### submit: packages
        conf = submit_objs['conf']

        for v in resources:
            if v['service'] == 's3a':
                service_url = 'http://{}:{}'.format(v['host'], v['port'])
                s3a = "org.apache.hadoop.fs.s3a.S3AFileSystem"

                conf.append(("spark.hadoop.fs.s3a.endpoint", service_url))
                conf.append(("spark.hadoop.fs.s3a.access.key", v['user']))
                conf.append(("spark.hadoop.fs.s3a.secret.key", v['password']))
                conf.append(("spark.hadoop.fs.s3a.impl", s3a))
                conf.append(("spark.hadoop.fs.s3a.path.style.access", "true"))
                break

        return submit_objs

    def set_submit_args(self):
        submit_args = ''

        for k in self.submit.keys() - {'conf'}:
            s = ",".join(self.submit[k])
            submit_args += f' --{k} {s}' if s else ''

        # submit config options one by one
        for c in self.submit['conf']:
            submit_args += f' --conf {c[0]}={c[1]}'

        #### print debug
        for k in self.submit.keys():
            if self.submit[k]:
                print(f'Configuring {k}:')
                for e in self.submit[k]:
                    v = e
                    if isinstance(e, tuple):
                        if len(e)>1 and str(e[0]).endswith('.key'):
                            e = (e[0], '****** (redacted)')
                        v = ' : '.join(list([str(x) for x in e]))
                    print(f'  -  {v}')

        # set PYSPARK_SUBMIT_ARGS env variable
        submit_args = '{} pyspark-shell'.format(submit_args)
        os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args

    def set_env_variables(self):
        for e in ['PYSPARK_PYTHON', 'PYSPARK_DRIVER_PYTHON']:
            if sys.executable and not os.environ.get(e):
                os.environ[e] = sys.executable

    def __init__(self, session_name=None, session_id=0, master = 'local[*]',
                 timezone=None, jars=None, packages=None, pyfiles=None, files=None,
                 repositories = None, services=None, conf=None) :

        #call base class
        # stop the previous instance,
        # register self a the new instance
        super().__init__('spark', session_name, session_id)

        # bundle all submit in a dictionary
        self.submit= {
            'jars': [jars] if isinstance(jars, str) else jars or [],
            'packages': [packages] if isinstance(packages, str) else packages or [],
            'py-files': [pyfiles] if isinstance(pyfiles, str) else pyfiles or [],
            'files': [files] if isinstance(files, str) else files or [],
            'repositories': [repositories] if isinstance(repositories, str) else repositories or [],
            'conf': [conf] if isinstance(conf, tuple) else conf or [],
        }

        # suppress INFO logging for java_gateway
        python_logging.getLogger('py4j.java_gateway').setLevel(python_logging.ERROR)

        # collect info
        self.set_info()

        # detect packages and configuration from services
        detected = self.detect_submit_params(services)

        # merge up with those passed with the init
        for k in self.submit.keys():
            self.submit[k] = list(sorted(set(self.submit[k] + detected[k])))

        #set submit args via env variable
        self.set_submit_args()

        # set other spark-related environment variables
        self.set_env_variables()

        # set spark conf object
        print(f"Connecting to spark master: {master}")

        conf = pyspark.SparkConf()
        self.set_conf_timezone(conf, timezone)

        # set session name
        conf.setAppName(session_name)

        # set master
        conf.setMaster(master)

        # config passed through the api call go via the config
        for c in self.submit['conf']:
            k,v,*_ = list(c)+['']
            if isinstance(v, (bool, int, float, str)):
                conf.set(k, v)

        # stop the current session if running
        self._stop()

        # start spark
        spark_session = self.start_context(conf)

        # record the data in the engine object for debug and future references
        self.conf = YamlDict(dict(conf.getAll()))

        if spark_session:
            self.conf = dict(dict(spark_session.sparkContext.getConf().getAll()))

            # set version if spark is loaded
            self._version = spark_session.version
            print(f'Engine context {self.engine_type}:{self.version} successfully started')

            # store the spark session
            self.context = spark_session

            # session is running
            self.stopped = False

    def initialize_spark_sql_context(self, spark_session, spark_context):
        try:
            del pyspark.sql.SQLContext._instantiatedContext
        except:
            pass

        if spark_context is None:
            spark_context = spark_session.sparkContext

        pyspark.sql.SQLContext._instantiatedContext = None
        sql_ctx = pyspark.sql.SQLContext(spark_context, spark_session)
        return sql_ctx


    def start_context(self, conf):
        try:
            # init the spark session
            session = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()

            # fix SQLContext for back compatibility
            self.initialize_spark_sql_context(session, session.sparkContext)

            # pyspark set log level method
            # (this will not suppress WARN before starting the context)
            session.sparkContext.setLogLevel("ERROR")

            # set the engine version
            self.version = session.version

            # set environment
            self.env = self.get_environment()

            return session
        except Exception as e:
            print(e)
            logging.error('Could not start the engine context')
            return None


    def get_environment(self):
        vars = [
            'SPARK_HOME',
            'HADOOP_HOME',
            'JAVA_HOME',
            'PYSPARK_PYTHON',
            'PYSPARK_DRIVER_PYTHON',
            'PYTHONPATH',
            'PYSPARK_SUBMIT_ARGS',
            'SPARK_DIST_CLASSPATH',
        ]

        return YamlDict({v: os.environ.get(v) for v in vars})


    def _stop(self, spark_session=None):
        self.stopped = True
        try:
            sc_from_session = spark_session.sparkContext if spark_session else None
            sc_from_engine = self.context.sparkContext if self.context else None
            sc_from_module = pyspark.SparkContext._active_spark_context or None

            scs = [
                sc_from_session,
                sc_from_engine,
                sc_from_module
            ]

            if self.context:
                self.context.stop()

            if spark_session:
                spark_session.stop()

            cls = pyspark.SparkContext

            for sc in scs:
                if sc:
                    try:
                        sc.stop()
                        sc._gateway.shutdown()
                    except Exception as e:
                        pass

            cls._active_spark_context = None
            cls._gateway = None
            cls._jvm = None
        except Exception as e:
            print(e)
            logging.warning(f'Could not fully stop the {self.engine_type} context')

    def load_log(self, md, options, ts_start):
        ts_end = timer()

        log_data = {
            'md': md,
            'options': options,
            'time': ts_end - ts_start
        }
        logging.info('load', extra=log_data)

    def load_with_pandas(self, kwargs):
        logging.warning("Fallback dataframe reader")

        # conversion of *some* pyspark arguments to pandas
        kwargs.pop('inferSchema', None)

        kwargs['header'] = 'infer' if kwargs.get('header') else None
        kwargs['prefix'] = '_c'

        return kwargs

    def load_csv(self, path=None, provider=None, *args,
                 sep=None, header=None, **kwargs):

        #return None
        obj = None

        md = Resource(
                path,
                provider,
                sep=sep,
                header=header,
                **kwargs)

        # download if necessary
        md = get_local(md)

        options =  md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['header'] = options.get('header') or True
        options['inferSchema'] = options.get('inferSchema') or True
        options['sep'] = options.get('sep') or ','

        local = self.is_spark_local()

        # start the timer for logging
        ts_start = timer()
        try:
            #three approaches: local, cluster, and service
            if md['service'] == 'file' and local:
                obj = self.context.read.options(**options).csv(md['url'])
            elif md['service'] == 'file':
                logging.warning(
                    f'local file + spark cluster: loading using pandas reader',
                    extra={'md': to_dict(md)})

                df = pd.read_csv(
                        md['url'],
                        sep=options['sep'],
                        header=options['header'])
                obj = self.context.createDataFrame(df)
            elif md['service'] in ['hdfs', 's3a']:
                obj = self.context.read.options(**options).csv(md['url'])
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
        except Exception as e:
            logging.error(e, extra={'md': md})

        self.load_log(md, options, ts_start)
        return obj


    def load_parquet(self, path=None, provider=None, *args,
                 mergeSchema=None, **kwargs):

        #return None
        obj = None

        md = Resource(
                path,
                provider,
                format='parquet',
                mergeSchema=mergeSchema,
                **kwargs)

        # download if necessary
        md = get_local(md)

        options =  md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['mergeSchema'] = options.get('mergeSchema') or True

        local = self.is_spark_local()

        # start the timer for logging
        ts_start = timer()
        try:
            #three approaches: local, cluster, and service
            if md['service'] == 'file' and local:
                obj = self.context.read.options(**options).parquet(md['url'])
            elif md['service'] == 'file':
                logging.warning(
                    f'local file + spark cluster: loading using pandas reader',
                    extra={'md': to_dict(md)})
                #fallback to the pandas reader, then convert to spark
                df = pd.read_parquet(md['url'])
                obj = self.context.createDataFrame(df)
            elif md['service'] in ['hdfs', 's3a']:
                obj = self.context.read.options(**options).parquet(md['url'])
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
        except Exception as e:
            logging.error(e, extra={'md': md})

        self.load_log(md, options, ts_start)
        return obj

    def load_json(self, path=None, provider=None, *args,
                 lines=True, **kwargs):

        #return None
        obj = None

        md = Resource(
                path,
                provider,
                format='json',
                lines=lines,
                **kwargs)

        # download if necessary
        md = get_local(md)

        options =  md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['lines'] = options.get('lines') or True
        options['inferSchema'] = options.get('inferSchema') or True

        local = self.is_spark_local()

        # start the timer for logging
        ts_start = timer()
        try:
            #three approaches: local, cluster, and service
            if md['service'] == 'file' and options['lines']:
                obj = self.context.read.options(**options).json(md['url'])
            elif md['service'] == 'file':
                # fallback to the pandas reader,
                # then convert to spark
                logging.warning(
                    f'local file + spark cluster: loading using pandas reader',
                    extra={'md': to_dict(md)})
                df = pd.read_json(
                        md['url'],
                        lines=options['lines'])
                obj = self.context.createDataFrame(df)
            elif md['service'] in ['hdfs', 's3a']:
                obj = self.context.read.options(**options).json(md['url'])
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
        except Exception as e:
            logging.error(e, extra={'md': md})

        self.load_log(md, options, ts_start)
        return obj

    def load_jdbc(self, path=None, provider=None, *args, **kwargs):
        #return None
        obj = None

        md = Resource(
                path,
                provider,
                format='jdbc',
                **kwargs)

        options =  md['options']

        # start the timer for logging
        ts_start = timer()
        try:
            if md['service'] in ['sqlite', 'mysql', 'postgres', 'mssql', 'clickhouse', 'oracle']:
                    obj = self.context.read \
                        .format('jdbc') \
                        .option('url', md['url']) \
                        .option("dbtable", md['table']) \
                        .option("driver", md['driver']) \
                        .option("user", md['user']) \
                        .option('password', md['password']) \
                        .options(**options)
                    # load the data from jdbc
                    obj = obj.load(**kwargs)
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})
                return obj

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
        except Exception as e:
            logging.error(e, extra={'md': md})

        self.load_log(md, options, ts_start)
        return obj


    def load(self, path=None, provider=None, *args, format=None, **kwargs):

        md = Resource(
                path,
                provider,
                format=format,
                **kwargs)

        if md['format'] == 'csv':
            return self.load_csv(path, provider, **kwargs)
        elif md['format'] == 'json':
            return self.load_json(path, provider, **kwargs)
        elif md['format'] == 'parquet':
            return self.load_parquet(path, provider, **kwargs)
        elif md['format'] == 'jdbc':
            return self.load_jdbc(path, provider, **kwargs)
        else:
            logging.error(
                    f'Unknown resource format "{md["format"]}"',
                    extra={'md': to_dict(md)})
        return None

    def save_log(self, md, options, ts_start):
        ts_end = timer()

        log_data = {
            'md': md,
            'options': options,
            'time': ts_end - ts_start
        }
        logging.info('save', extra=log_data)

    def is_spark_local(self):
        return self.conf.get('spark.master').startswith('local[')

    def directory_to_file(self, path):
        if os.path.exists(path) and os.path.isfile(path):
            return

        dirname = os.path.dirname(path)
        basename = os.path.basename(path)

        filename = list(filter(lambda x: x.startswith('part-'), os.listdir(path)))
        if len(filename) != 1:
            if len(filename)>1:
                logging.warning('cannot convert if more than a partition present')
            return
        else:
            filename = filename[0]

        shutil.move(os.path.join(path, filename), dirname)
        if os.path.exists(path) and os.path.isdir(path):
            shutil.rmtree(path)

        shutil.move(os.path.join(dirname, filename), os.path.join(dirname, basename))
        return

    def save_parquet(self, obj, path=None, provider=None, *args,
                 mode=None, **kwargs):

        result = True
        md = Resource(
                path,
                provider,
                format='parquet',
                mode=mode,
                **kwargs)
        options = md['options']

        # after collecting from metadata, or method call, define defaults
        options['mode'] = options['mode'] or 'overwrite'

        local = self.is_spark_local()

        ts_start = timer()
        try:
            #three approaches: file-local, local+cluster, and service
            if md['service'] == 'file' and local:
                obj.coalesce(1).write\
                    .format('parquet')\
                    .mode(options['mode'])\
                    .options(**options)\
                    .parquet(md['url'])

            elif md['service'] == 'file':
                if os.path.exists(md['url']) and os.path.isdir(md['url']):
                    shutil.rmtree(md['url'])

                # save with pandas
                obj.toPandas().to_parquet(
                    md['url'],
                    mode=options['mode'])

            elif md['service'] in ['hdfs', 's3a']:
               obj.write\
                    .format('parquet')\
                    .mode(options['mode'])\
                    .options(**options)\
                    .parquet(md['url'])
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})
                result = False

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
            result = False

        except Exception as e:
            logging.error({'md': md, 'error_msg': str(e)})
            raise e

        self.save_log(md, options, ts_start)
        return result

    def save_csv(self, obj, path=None, provider=None, *args,
                 mode=None, sep=None, header=None, **kwargs):

        result = True

        md = Resource(
                path,
                provider,
                format='csv',
                mode=mode,
                sep=sep,
                header=header,
                **kwargs)

        options = md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['header'] = options['header'] or 'true'
        options['sep'] = options['sep'] or ','
        options['mode'] = options['mode'] or 'overwrite'

        local = self.is_spark_local()

        ts_start = timer()
        try:
            #three approaches: file+local, file+cluster, and service
            if md['service'] == 'file' and local:
                obj.coalesce(1).write\
                    .format('csv')\
                    .mode(options['mode'])\
                    .options(**options)\
                    .csv(md['url'])
                self.directory_to_file(md['url'])

            elif md['service'] == 'file':
                if os.path.exists(md['url']) and os.path.isdir(md['url']):
                    shutil.rmtree(md['url'])

                # save with pandas
                obj.toPandas().to_csv(
                    md['url'],
                    mode=options['mode'],
                    header=options['header'],
                    sep=options['sep'])

            elif md['service'] in ['hdfs', 's3a']:
                obj.write\
                    .format('csv')\
                    .mode(options['mode'])\
                    .options(**options)\
                    .csv(md['url'])
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})
                result = False

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
            result = False

        except Exception as e:
            logging.error({'md': md, 'error_msg': str(e)})
            raise e

        self.save_log(md, options, ts_start)
        return result


    def save_json(self, obj, path=None, provider=None, *args,
                 mode=None, lines=None, **kwargs):

        result = True

        md = Resource(
                path,
                provider,
                format='csv',
                mode=mode,
                lines=lines,
                **kwargs)

        options = md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['mode'] = options['mode'] or 'overwrite'
        options['lines'] = options['lines'] or True

        local = self.is_spark_local()

        ts_start = timer()
        try:
            #three approaches: local, cluster, and service
            if local and md['service'] == 'file' and options['lines']:
                obj.coalesce(1).write\
                    .format('json')\
                    .mode(options['mode'])\
                    .options(**options)\
                    .json(md['url'])
                self.directory_to_file(md['url'])

            elif md['service'] == 'file':
                # fallback, use pandas
                # save single files, not directories
                if os.path.exists(md['url']) and os.path.isdir(md['url']):
                    shutil.rmtree(md['url'])

                # save with pandas
                obj.toPandas().to_json(
                    md['url'],
                    mode=options['mode'],
                    lines=options['lines'])

            elif md['service'] in ['hdfs', 's3a']:
                obj.write\
                    .format('json')\
                    .mode(options['mode'])\
                    .options(**options)\
                    .json(md['url'])
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})
                result = False

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
            result = False

        except Exception as e:
            logging.error({'md': md, 'error_msg': str(e)})
            raise e

        self.save_log(md, options, ts_start)
        return result

    def save_jdbc(self, obj, path=None, provider=None, *args, mode=None, **kwargs):

        result = True
        md = Resource(
                path,
                provider,
                format='jdbc',
                mode=mode,
                **kwargs)

        options = md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['mode'] = options['mode'] or 'overwrite'

        ts_start = timer()
        try:
            #three approaches: local, cluster, and service
            if md['service'] in ['sqlite', 'mysql', 'postgres', 'mssql', 'clickhouse', 'oracle']:
                obj.write \
                    .format('jdbc') \
                    .option('url', md['url']) \
                    .option("dbtable", md['table']) \
                    .option("driver", md['driver']) \
                    .option("user", md['user']) \
                    .option('password', md['password']) \
                    .options(**options) \
                    .mode(options['mode'])\
                    .save()
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})
                result = False

        except AnalysisException as e:
            logging.error(str(e), extra={'md': md})
            result = False

        except Exception as e:
            logging.error({'md': md, 'error_msg': str(e)})
            raise e

        self.save_log(md, options, ts_start)
        return result

    def save(self, obj, path=None, provider=None, *args, format=None, mode=None, **kwargs):

        md = Resource(
                path,
                provider,
                format=format,
                mode=mode,
                **kwargs)

        if md['format'] == 'csv':
            return self.save_csv(obj, path, provider, mode=mode, **kwargs)
        elif md['format'] == 'tsv':
            kwargs['sep'] = '\t'
            return self.save_csv(obj, path, provider, mode=mode, **kwargs)
        elif md['format'] == 'json':
            return self.save_json(obj, path, provider, mode=mode, **kwargs)
        elif md['format'] == 'jsonl':
            return self.save_json(obj, path, provider, mode=mode, **kwargs)
        elif md['format'] == 'parquet':
            return self.save_parquet(obj, path, provider, mode=mode, **kwargs)
        elif md['format'] == 'jdbc':
            return self.save_jdbc(obj, path, provider, mode=mode, **kwargs)
        else:
            logging.error(f'Unknown format "{md["service"]}"', extra={'md':md})
            return False

    def copy(self, md_src, md_trg, mode='append'):
        # timer
        timer_start = timer()

        # src dataframe
        df_src = self.load(md_src)

        # if not path on target, get it from src
        if not md_trg['resource_path']:
            md_trg = resource.metadata(
                self._rootdir,
                self._metadata,
                md_src['resource_path'],
                md_trg['provider_alias'])

        # logging
        log_data = {
            'src_hash': md_src['hash'],
            'src_path': md_src['resource_path'],
            'trg_hash': md_trg['hash'],
            'trg_path': md_trg['resource_path'],
            'mode': mode,
            'updated': False,
            'records_read': 0,
            'records_add': 0,
            'records_del': 0,
            'columns': 0,
            'time': timer() - timer_start
        }

        # could not read source, log error and return
        if df_src is None:
            logging.error(log_data)
            return

        num_rows = df_src.count()
        num_cols = len(df_src.columns)

        # empty source, log notice and return
        if num_rows == 0 and mode == 'append':
            log_data['time'] = timer() - timer_start
            logging.notice(log_data)
            return

        # overwrite target, save, log notice/error and return
        if mode == 'overwrite':
            if md_trg['state_column']:
                df_src = df_src.withColumn('_state', F.lit(0))

            result = self.save(df_src, md_trg, mode=mode)

            log_data['time'] = timer() - timer_start
            log_data['records_read'] = num_rows
            log_data['records_add'] = num_rows
            log_data['columns'] = num_cols

            logging.notice(log_data) if result else logging.error(log_data)
            return

        # trg dataframe (if exists)
        try:
            df_trg = self.load(md_trg, catch_exception=False)
        except:
            df_trg = dataframe.empty(df_src)

        # de-dup (exclude the _updated column)

        # create a view from the extracted log
        df_trg = dataframe.view(df_trg)

        # capture added records
        df_add = dataframe.diff(df_src, df_trg, ['_date', '_datetime', '_updated', '_hash', '_state'])
        rows_add = df_add.count()

        # capture deleted records
        rows_del = 0
        if md_trg['state_column']:
            df_del = dataframe.diff(df_trg, df_src, ['_date', '_datetime', '_updated', '_hash', '_state'])
            rows_del = df_del.count()

        updated = (rows_add + rows_del) > 0

        num_cols = len(df_add.columns)
        num_rows = max(df_src.count(), df_trg.count())

        # save diff
        if updated:
            if md_trg['state_column']:
                df_add = df_add.withColumn('_state', F.lit(0))
                df_del = df_del.withColumn('_state', F.lit(1))

                df = df_add.union(df_del)
            else:
                df = df_add

            result = self.save(df, md_trg, mode=mode)
        else:
            result = True

        log_data.update({
            'updated': updated,
            'records_read': num_rows,
            'records_add': rows_add,
            'records_del': rows_del,
            'columns': num_cols,
            'time': timer() - timer_start
        })

        logging.notice(log_data) if result else logging.error(log_data)


    def list(self, provider, path=''):
        df_schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('type', T.StringType(), True)])

        df_empty = self._ctx.createDataFrame(data=(), schema=df_schema)

        if isinstance(provider, str):
            md = resource.metadata(self._rootdir, self._metadata, None, provider)
        elif isinstance(provider, dict):
            md = provider
        else:
            logging.warning(f'{str(provider)} cannot be used to reference a provider')
            return df_empty

        try:
            if md['service'] in ['local', 'file']:
                lst = []
                rootpath = os.path.join(md['provider_path'], path)
                for f in os.listdir(rootpath):
                    fullpath = os.path.join(rootpath, f)
                    if os.path.isfile(fullpath):
                        obj_type = 'FILE'
                    elif os.path.isdir(fullpath):
                        obj_type = 'DIRECTORY'
                    elif os.path.islink(fullpath):
                        obj_type = 'LINK'
                    elif os.path.ismount(fullpath):
                        obj_type = 'MOUNT'
                    else:
                        obj_type = 'UNDEFINED'

                    obj_name = f
                    lst += [(obj_name, obj_type)]

                if lst:
                    df = self._ctx.createDataFrame(lst, ['name', 'type'])
                else:
                    df = df_empty

                return df

            elif md['service'] in ['hdfs', 'minio', 's3a']:
                sc = self._ctx._sc
                URI = sc._gateway.jvm.java.net.URI
                Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
                FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
                fs = FileSystem.get(URI(md['url']), sc._jsc.hadoopConfiguration())

                provider_path = md['provider_path'] if md['service'] == 'hdfs' else '/'
                obj = fs.listStatus(Path(os.path.join(provider_path, path)))

                lst = []

                for i in range(len(obj)):
                    if obj[i].isFile():
                        obj_type = 'FILE'
                    elif obj[i].isDirectory():
                        obj_type = 'DIRECTORY'
                    else:
                        obj_type = 'UNDEFINED'

                    obj_name = obj[i].getPath().getName()
                    lst += [(obj_name, obj_type)]

                if lst:
                    df = self._ctx.createDataFrame(lst, ['name', 'type'])
                else:
                    df = df_empty

                return df

            elif md['format'] == 'jdbc':
                # remove options from database, if any
                database = md["database"].split('?')[0]
                schema = md['schema']
                if md['service'] == 'mssql':
                    query = f"""
                            ( SELECT table_name, table_type
                              FROM INFORMATION_SCHEMA.TABLES
                              WHERE table_schema='{schema}'
                            ) as query
                            """
                elif md['service'] == 'oracle':
                    query = f"""
                            ( SELECT table_name, table_type
                             FROM all_tables
                             WHERE table_schema='{schema}'
                            ) as query
                            """
                elif md['service'] == 'mysql':
                    query = f"""
                            ( SELECT table_name, table_type
                              FROM information_schema.tables
                              WHERE table_schema='{schema}'
                            ) as query
                            """
                elif md['service'] == 'postgres':
                    query = f"""
                            ( SELECT table_name, table_type
                              FROM information_schema.tables
                              WHERE table_schema = '{schema}'
                            ) as query
                            """
                else:
                    # vanilla query ... for other databases
                    query = f"""
                                ( SELECT table_name, table_type
                                  FROM information_schema.tables'
                                ) as query
                                """

                obj = self._ctx.read \
                    .format('jdbc') \
                    .option('url', md['url']) \
                    .option("dbtable", query) \
                    .option("driver", md['driver']) \
                    .option("user", md['username']) \
                    .option('password', md['password']) \
                    .load()

                # load the data from jdbc
                lst = []
                for x in obj.select('TABLE_NAME', 'TABLE_TYPE').collect():
                    lst.append((x.TABLE_NAME, x.TABLE_TYPE))

                if lst:
                    df = self._ctx.createDataFrame(lst, ['name', 'type'])
                else:
                    df = df_empty

                return df

            else:
                logging.error({'md': md, 'error_msg': f'List resource on service "{md["service"]}" not implemented'})
                return df_empty
        except Exception as e:
            logging.error({'md': md, 'error_msg': str(e)})
            raise e

        return df_empty
