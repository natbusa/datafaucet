import sys
import os, time
import shutil
import textwrap

from pathlib import Path
            
import logging as python_logging
from abc import ABC

from datafaucet import logging

from datafaucet.resources import Resource, get_local, urnparse

from datafaucet.yaml import YamlDict, to_dict
from datafaucet.utils import merge, python_version, get_tool_home, run_command

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


def get_hadoop_version_from_system():
    hadoop_home = get_tool_home('hadoop', 'HADOOP_HOME', 'bin')[0]
    hadoop_abspath = os.path.join(hadoop_home, 'bin', 'hadoop')

    try:
        output = run_command(f'{hadoop_abspath}', 'version')
        return output[0].split()[1]
    except:
        return ''


def to_pandas_args(kwargs):
    # conversion of *some* pyspark arguments to pandas
    kwargs.pop('inferSchema', None)

    kwargs['header'] = 'infer' if kwargs.get('header') else None
    kwargs['prefix'] = '_c'

    return kwargs


def initialize_spark_sql_context(spark_session, spark_context):
    try:
        del pyspark.sql.SQLContext._instantiatedContext
    except:
        pass

    if spark_context is None:
        spark_context = spark_session.sparkContext

    pyspark.sql.SQLContext._instantiatedContext = None
    sql_ctx = pyspark.sql.SQLContext(spark_context, spark_session)
    return sql_ctx


def directory_to_file(path):
    if os.path.exists(path) and os.path.isfile(path):
        return

    dirname = os.path.dirname(path)
    basename = os.path.basename(path)

    filename = list(filter(lambda x: x.startswith('part-'), os.listdir(path)))
    if len(filename) != 1:
        if len(filename) > 1:
            logging.warning(
                'In local mode, ',
                'save will not flatten the directory to file,',
                'if more than a partition present')
        return
    else:
        filename = filename[0]

    shutil.move(os.path.join(path, filename), dirname)
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)

    shutil.move(os.path.join(dirname, filename), os.path.join(dirname, basename))
    return


class SparkEngine(EngineBase, metaclass=EngineSingleton):

    @staticmethod
    def set_conf_timezone(conf, timezone=None):
        assert (type(conf) == pyspark.conf.SparkConf)

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
            self.stop(spark_session)
        except Exception as e:
            pass

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
        services = [services] if isinstance(services, str) else services
        services = services or []

        # if service is a string, make a resource out of it

        resources = [s if isinstance(s, dict) else Resource(service=s) for s in services]

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

        # submit: repositories
        repositories = submit_objs['repositories']

        # submit: jars
        jars = submit_objs['jars']

        # submit: packages
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
            elif s == 'file':
                pass
            elif s == 'hdfs':
                pass
            else:
                logging.warning(f'could not autodetect driver to install for {s}, version {v}')

        # submit: packages
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
                conf.append(("spark.hadoop.fs.s3a.buffer.dir","/tmp/hadoop.fs.s3a.buffer.dir"))
                conf.append(("spark.hadoop.fs.s3a.block.size","64M"))
                conf.append(("spark.hadoop.fs.s3a.multipart.size","64M")) # size of each multipart chunk
                conf.append(("spark.hadoop.fs.s3a.multipart.threshold","64M")) # size before using multipart uploads
                conf.append(("spark.hadoop.fs.s3a.fast.upload.active.blocks","2048")) # 2048 number of parallel uploads
                conf.append(("spark.hadoop.fs.s3a.fast.upload.buffer","disk")) # use disk as the buffer for uploads
                conf.append(("spark.hadoop.fs.s3a.fast.upload","true")) # turn on fast upload mode
                
                conf.append(("spark.hadoop.fs.s3a.connection.establish.timeout","5000"))
                conf.append(("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"))
                conf.append(("spark.hadoop.fs.s3a.connection.timeout", "200000"))
                conf.append(("spark.hadoop.fs.s3a.connection.maximum","8192")) # maximum number of concurrent conns
 
                conf.append(("spark.hadoop.fs.s3a.committer.magic.enabled","false"))
                conf.append(("spark.hadoop.fs.s3a.committer.name","partitioned"))
                conf.append(("spark.hadoop.fs.s3a.committer.staging.abort.pending.uploads","true"))
                conf.append(("spark.hadoop.fs.s3a.committer.staging.conflict-mode","append"))
                conf.append(("spark.hadoop.fs.s3a.committer.staging.tmp.path","/tmp/staging"))
                conf.append(("spark.hadoop.fs.s3a.committer.staging.unique-filenames","true"))

                conf.append(("spark.hadoop.fs.s3a.committer.threads","2048")) # 2048 number of threads writing to MinIO
                conf.append(("spark.hadoop.fs.s3a.max.total.tasks","2048")) # maximum number of parallel tasks
                conf.append(("spark.hadoop.fs.s3a.threads.max","2048")) # maximum number of threads for S3A

                conf.append(("spark.hadoop.fs.s3a.socket.recv.buffer","65536")) # read socket buffer hint
                conf.append(("spark.hadoop.fs.s3a.socket.send.buffer","65536")) # write socket buffer hint
                
                conf.append(("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2"))
                conf.append(("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored","true"))
                
                conf.append(("spark.sql.sources.commitProtocolClass","org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"))
                conf.append(("spark.sql.parquet.output.committer.class","org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"))

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

        # print debug
        for k in self.submit.keys():
            if self.submit[k]:
                logging.notice(f'Configuring {k}:')
                for e in self.submit[k]:
                    v = e
                    if isinstance(e, tuple):
                        if len(e) > 1 and str(e[0]).endswith('.key'):
                            e = (e[0], '****** (redacted)')
                        v = ' : '.join(list([str(x) for x in e]))
                    if k=='conf':
                        logging.info(f'  -  {v}')
                    else:
                        logging.notice(f'  -  {v}')
                        
        # set PYSPARK_SUBMIT_ARGS env variable
        submit_args = '{} pyspark-shell'.format(submit_args)
        os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args

    def set_env_variables(self):
        for e in ['PYSPARK_PYTHON', 'PYSPARK_DRIVER_PYTHON']:
            if sys.executable and not os.environ.get(e):
                os.environ[e] = sys.executable

    def __init__(self, session_name=None, session_id=0, master='local[*]',
                 timezone=None, repositories=None, jars=None, packages=None, files=None,
                 services=None, conf=None, detect=True):

        # call base class
        # stop the previous instance,
        # register self a the new instance
        super().__init__('spark', session_name, session_id)

        # bundle all submit in a dictionary
        self.submit = {
            'jars': [jars] if isinstance(jars, str) else jars or [],
            'packages': [packages] if isinstance(packages, str) else packages or [],
            'files': [files] if isinstance(files, str) else files or [],
            'repositories': [repositories] if isinstance(repositories, str) else repositories or [],
            'conf': [conf] if isinstance(conf, tuple) else conf or [],
        }

        # suppress INFO logging for java_gateway
        python_logging.getLogger('py4j.java_gateway').setLevel(python_logging.ERROR)

        # collect info
        self.set_info()

        # detect packages and configuration from services
        if detect:
            detected = self.detect_submit_params(services)
            self.submit = merge(detected, self.submit)

        # set submit args via env variable
        self.set_submit_args()

        # set other spark-related environment variables
        self.set_env_variables()

        # set spark conf object
        logging.notice(f"Connecting to spark master: {master}")

        conf = pyspark.SparkConf()
        self.set_conf_timezone(conf, timezone)

        # set session name
        conf.setAppName(session_name)

        # set master
        conf.setMaster(master)

        # config passed through the api call go via the config
        for c in self.submit['conf']:
            k, v, *_ = list(c) + ['']
            if isinstance(v, (bool, int, float, str)):
                conf.set(k, v)

        # stop the current session if running
        self.stop()

        # start spark
        spark_session = self.start_context(conf)

        # record the data in the engine object for debug and future references
        self.conf = YamlDict(dict(conf.getAll()))

        if spark_session:
            self.conf = dict(dict(spark_session.sparkContext.getConf().getAll()))

            # set version if spark is loaded
            self._version = spark_session.version
            logging.notice(f'Engine context {self.engine_type}:{self.version} successfully started')

            # store the spark session
            self.context = spark_session

            # session is running
            self.stopped = False

    def start_context(self, conf):
        try:
            # init the spark session
            session = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()

            # fix SQLContext for back compatibility
            initialize_spark_sql_context(session, session.sparkContext)

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

    def stop(self, spark_session=None):
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

    def range(self, *args):
        return self.context.range(*args)

    def load_log(self, md, options, ts_start):
        ts_end = timer()

        log_data = {
            'md': md,
            'options': options,
            'time': ts_end - ts_start
        }
        logging.info('load', extra=log_data)

    def load_csv(self, path=None, provider=None, *args,
                 sep=None, header=None, **kwargs):

        # return None
        obj = None

        md = Resource(
            path,
            provider,
            sep=sep,
            header=header,
            **kwargs)

        # download if necessary
        md = get_local(md)

        options = md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['header'] = options.get('header') or True
        options['inferSchema'] = options.get('inferSchema') or True
        options['sep'] = options.get('sep') or ','

        local = self.is_spark_local()

        # start the timer for logging
        ts_start = timer()
        try:
            # three approaches: local, cluster, and service
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

        # return None
        obj = None

        md = Resource(
            path,
            provider,
            format='parquet',
            mergeSchema=mergeSchema,
            **kwargs)

        # download if necessary
        md = get_local(md)

        options = md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['mergeSchema'] = options.get('mergeSchema') or True

        local = self.is_spark_local()

        # start the timer for logging
        ts_start = timer()
        try:
            # three approaches: local, cluster, and service
            if md['service'] == 'file' and local:
                obj = self.context.read.options(**options).parquet(md['url'])
            elif md['service'] == 'file':
                logging.warning(
                    f'local file + spark cluster: loading using pandas reader',
                    extra={'md': to_dict(md)})
                # fallback to the pandas reader, then convert to spark
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

        # return None
        obj = None

        md = Resource(
            path,
            provider,
            format='json',
            lines=lines,
            **kwargs)

        # download if necessary
        md = get_local(md)

        options = md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['lines'] = options.get('lines') or True
        options['inferSchema'] = options.get('inferSchema') or True

        local = self.is_spark_local()

        # start the timer for logging
        ts_start = timer()
        try:
            # three approaches: local, cluster, and service
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
        # return None
        obj = None

        md = Resource(
            path,
            provider,
            format='jdbc',
            **kwargs)

        options = md['options']

        # start the timer for logging
        ts_start = timer()
        
        # avoid multi-processing and distributed writes on sqlite
        if md['service'] == 'sqlite':
            local = self.is_spark_local()
            if not local:
                raise ValueError('load to sqlite can only be done from a local cluster')
                #todo:
                # sketched solution obj.toPandas().to_sql(md['url']
            
        try:
            if md['service'] in ['sqlite', 'mysql', 'postgres', 'mssql', 'clickhouse', 'oracle']:
                obj = self.context.read \
                    .format('jdbc') \
                    .option('url', md['url']) \
                    .option("dbtable", md['table']) \
                    .option("driver", md['driver'])

                if md['user']:
                    obj = obj.option("user", md['user'])
                    
                if md['password']:
                    obj = obj.option('password', md['password'])

                obj = obj.options(**options)
                
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

    def load_scd(self, path, provider, format=None, merge_on=None, version=None, where=None, **kwargs):
        where = where or []
        where = where if isinstance(where, (list, tuple)) else [where]

        obj = self.load(path, provider, format=format, **kwargs)

        # push down filters asap
        for predicate in where:
            obj = obj.filter(predicate)

        # create view from history log
        return dataframe.view(obj, merge_on=merge_on, version=version)

    def load(self, path=None, provider=None, *args, format=None, **kwargs):

        md = Resource(
            path,
            provider,
            format=format,
            **kwargs)

        format, _, storage_format = md['format'].partition(':')
        if format == 'scd':
            return self.load_scd(path, provider, format=storage_format, **kwargs)

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

    def save_parquet(self, obj, path=None, provider=None, *args, mode=None, **kwargs):

        result = True
        md = Resource(
            path,
            provider,
            format='parquet',
            mode=mode,
            **kwargs)
        options = md['options']

        # after collecting from metadata, or method call, define defaults
        options['mode'] = options.get('mode', None) or 'overwrite'

        pcols = options.pop('partitionBy', None) or []
        pcols = pcols if isinstance(pcols, (list, tuple)) else [pcols]

        local = self.is_spark_local()

        ts_start = timer()
        try:
            # three approaches: file-local, local+cluster, and service
            if md['service'] == 'file' and local:
                obj.coalesce(1).write \
                    .partitionBy(*pcols) \
                    .format('parquet') \
                    .mode(options['mode']) \
                    .options(**options) \
                    .parquet(md['url'], **options)

            elif md['service'] == 'file':
                if os.path.exists(md['url']) and os.path.isdir(md['url']):
                    shutil.rmtree(md['url'])

                # save with pandas
                obj.toPandas().to_parquet(
                    md['url'],
                    mode=options['mode'])

            elif md['service'] in ['hdfs', 's3a']:
                obj.write \
                    .partitionBy(*pcols) \
                    .format('parquet') \
                    .mode(options['mode']) \
                    .options(**options) \
                    .parquet(md['url'], **options)
            else:
                logging.error(
                    f'Unknown resource service "{md["service"]}"',
                    extra={'md': to_dict(md)})
                result = False

        except AnalysisException as e:
            logging.error(' '.join(str(e).split('/n')[:2])[:300], extra={'md': md})
            result = False

        except Exception as e:
            logging.error(' '.join(str(e).split('/n')[:2])[:300], extra={'md': md, 'error_msg': str(e)})
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
        options['header'] = options.get('header', None) or 'true'
        options['sep'] = options.get('sep', None) or ','
        options['mode'] = options.get('mode', None) or 'overwrite'

        pcols = options.pop('partitionBy', None) or []
        pcols = pcols if isinstance(pcols, (list, tuple)) else [pcols]

        local = self.is_spark_local()

        ts_start = timer()
        try:
            # three approaches: file+local, file+cluster, and service
            if md['service'] == 'file' and local:
                obj.coalesce(1).write \
                    .partitionBy(*pcols) \
                    .format('csv') \
                    .mode(options['mode']) \
                    .options(**options) \
                    .csv(md['url'], **options)
                directory_to_file(md['url'])

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
                obj.write \
                    .partitionBy(*pcols) \
                    .format('csv') \
                    .mode(options['mode']) \
                    .options(**options) \
                    .csv(md['url'], **options)
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
        options['mode'] = options.get('mode', None) or 'overwrite'
        options['lines'] = options.get('lines', None) or True

        pcols = options.pop('partitionBy', None) or []
        pcols = pcols if isinstance(pcols, (list, tuple)) else [pcols]

        local = self.is_spark_local()

        ts_start = timer()
        try:
            # three approaches: local, cluster, and service
            if local and md['service'] == 'file' and options['lines']:
                obj.coalesce(1).write \
                    .partitionBy(*pcols) \
                    .format('json') \
                    .mode(options['mode']) \
                    .options(**options) \
                    .json(md['url'])
                directory_to_file(md['url'])

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
                obj.write \
                    .partitionBy(*pcols) \
                    .format('json') \
                    .mode(options['mode']) \
                    .options(**options) \
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
        options['mode'] = options.get('mode', None) or 'overwrite'
        
        # avoid multi-processing and distributed writes on sqlite
        if md['service'] == 'sqlite':
            local = self.is_spark_local()
            if not local:
                raise ValueError('write to sqlite can only be done from a local cluster')
                #todo:
                # sketched solution obj.toPandas().to_sql(md['url']
            
            #calesce to a single writer
            obj = obj.coalesce(1)


        # partition is meaningless here
        pcols = options.pop('partitionBy', None) or []

        ts_start = timer()
        try:
            if md['service'] in ['sqlite', 'mysql', 'postgres', 'mssql', 'clickhouse', 'oracle']:
                obj = obj.write \
                    .format('jdbc') \
                    .option('url', md['url']) \
                    .option("dbtable", md['table']) \
                    .option("driver", md['driver'])
                
                if md['user']:
                    obj = obj.option("user", md['user'])
                    
                if md['password']:
                    obj = obj.option('password', md['password'])
                    
                obj.options(**options).mode(options['mode']).save()
                
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

        format, _, storage_format = md['format'].partition(':')

        if format == 'scd':
            return self.save_scd(obj, path, provider, format=storage_format, mode=mode, **kwargs)

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
            logging.error(f'Unknown format "{md["service"]}"', extra={'md': md})
            return False

    def save_scd(self, obj, path=None, provider=None, *args, format=None, mode=None, merge_on=None, where=None,
                 **kwargs):

        result = True
        md = Resource(
            path,
            provider,
            format=format,
            mode=mode,
            **kwargs)

        options = md['options']

        # after collecting from metadata, or method call, define csv defaults
        options['mode'] = options.get('mode', None) or 'append'
        format = md['format'] or 'parquet'

        where = where or []
        where = where if isinstance(where, (list, tuple)) else [where]

        ts_start = timer()

        num_rows = obj.count()
        num_cols = len(obj.columns)

        # empty source, log notice and return
        if num_rows == 0 and mode == 'append':
            return True

        # overwrite target, save, log notice/error and return
        if options['mode'] == 'overwrite':
            obj = obj.withColumn('_state', F.lit(0))
            obj = dataframe.add_update_column(obj, '_updated')

            result = self.save(obj, md, mode=options['mode'])
            return True

        # append
        df_src = obj

        # trg dataframe (if exists)
        try:
            df_trg = self.load(md, format=format, catch_exception=False)
        except:
            df_trg = dataframe.empty(df_src)

        if '_state' not in df_trg.columns:
            df_trg = df_trg.withColumn('_state', F.lit(0))

        if '_updated' not in df_trg.columns:
            df_trg = dataframe.add_update_column(df_trg, '_updated')

        # filter src and trg (mainly speed reason: reduce diff time, but compare only a portion of all records)
        for predicate in where:
            df_src = df_src.filter(predicate)
            df_trg = df_trg.filter(predicate)

        # create a view from the extracted log
        df_trg = dataframe.view(df_trg, merge_on=merge_on)

        # schema change: add new columns
        added_cols = set(df_src.columns) - set(df_trg.columns)
        added_cols = {x.name: x.dataType for x in list(df_src.schema) if x.name in added_cols}
        for c, t in added_cols.items():
            df_trg = df_trg.withColumn(c, F.lit(None).cast(t))

        # schema change: removed columns
        # no need to do anything, diff will take care of that

        # capture added records
        df_add = dataframe.diff(df_src, df_trg, ['_updated', '_state'])

        # capture deleted records
        df_del = dataframe.diff(df_trg, df_src, ['_updated', '_state'])

        # capture updated records
        cnt_upd = 0
        if merge_on is not None:
            on = merge_on if isinstance(merge_on, (list, tuple)) else [merge_on]
            cnt_upd = df_add.join(df_del, on=on).count()

        cnt_del = df_del.count() - cnt_upd
        cnt_add = df_add.count() - cnt_upd

        logging.notice(f'merge on={merge_on}, updated={cnt_upd}, added={cnt_add}, deleted={cnt_del}')

        df_add = df_add.withColumn('_state', F.lit(0))
        df_del = df_del.withColumn('_state', F.lit(1))

        df = df_add.union(df_del)
        df = dataframe.add_update_column(df, '_updated')

        result = self.save(df, md, format=format, **options)

        self.save_log(md, options, ts_start)
        return result

    def list(self, provider, path=None, **kwargs):
        df_schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('type', T.StringType(), True)])

        df_empty = self.context.createDataFrame(data=(), schema=df_schema)

        md = Resource(path, provider, **kwargs)

        try:
            if md['service'] in ['local', 'file']:
                lst = []
                rootpath = md['url']
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
                    df = self.context.createDataFrame(lst, ['name', 'type'])
                else:
                    df = df_empty

                return df

            elif md['service'] in ['hdfs', 's3a']:
                sc = self.context._sc
                URI = sc._gateway.jvm.java.net.URI
                Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
                FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

                parsed = urnparse(md['url'])
                if md['service'] == 's3a':
                    path = parsed.path.split('/')
                    url = 's3a://' + path[0]
                    path = '/' + '/'.join(path[1:]) if len(path) > 1 else '/'

                if md['service'] == 'hdfs':
                    host_port = f"{parsed.host}:{parsed.port}" if parsed.port else parsed.hosts
                    url = f'hdfs://{host_port}'
                    path = '/' + parsed.path

                try:
                    fs = FileSystem.get(URI(url), sc._jsc.hadoopConfiguration())
                    obj = fs.listStatus(Path(path))
                except:
                    logging.error(f'An error occurred accessing {url}{path}')
                    obj = []

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
                    df = self.context.createDataFrame(lst, ['name', 'type'])
                else:
                    df = df_empty

                return df

            elif md['format'] == 'jdbc':
                # remove options from database, if any

                database = md["database"].split('?')[0]
                schema = md['schema']
                table = md['table']

                if database and table:
                    try:
                        obj = self.context.read \
                            .format('jdbc') \
                            .option('url', md['url']) \
                            .option("dbtable", table) \
                            .option("driver", md['driver']) \
                            .option("user", md['user']) \
                            .option('password', md['password']) \
                            .load()
                        info = [(i.name, i.dataType.simpleString()) for i in obj.schema]
                    except:
                        info = []

                    if info:
                        return self.context.createDataFrame(info, ['name', 'type'])

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

                obj = self.context.read \
                    .format('jdbc') \
                    .option('url', md['url']) \
                    .option("dbtable", query) \
                    .option("driver", md['driver']) \
                    .option("user", md['user']) \
                    .option('password', md['password']) \
                    .load()

                # load the data from jdbc
                lst = []
                for x in obj.select('TABLE_NAME', 'TABLE_TYPE').collect():
                    lst.append((x.TABLE_NAME, x.TABLE_TYPE))

                if lst:
                    df = self.context.createDataFrame(lst, ['name', 'type'])
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
