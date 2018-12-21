# import os
#
# from datalabframework.spark.mapping import transform as mapping_transform
# from datalabframework.spark.filter import transform as filter_transform
# from datalabframework.spark.diff import dataframe_update
#
# from datalabframework import project
# from datalabframework import metadata
# from datalabframework import logging
# from datalabframework._utils import ImmutableDict, merge
#
# from datalabframework import spark as sparkfun
#
# from datetime import datetime
#
# import pyspark
#
#
# # purpose of engines
# # abstract engine init, data read and data write
# # and move this information to metadata
#
# # it does not make the code fully engine agnostic though.
#
# import sys
#
# def func_name():
#     # noinspection PyProtectedMember
#     return sys._getframe(1).f_code.co_name
#
# class SparkEngine:
#     def __init__(self, name, config):
#         from pyspark import SparkContext, SparkConf
#         from pyspark.sql import SQLContext
#
#         submit_args = ''
#
#         jars = []
#         jars += config.get('jars', [])
#         if jars:
#             submit_jars = ' '.join(jars)
#             submit_args = '{} --jars {}'.format(submit_args, submit_jars)
#
#         packages = config.get('packages', [])
#         if packages:
#             submit_packages = ','.join(packages)
#             submit_args = '{} --packages {}'.format(submit_args, submit_packages)
#
#         pyfiles = config.get('py-files', [])
#         if pyfiles:
#             submit_pyfiles = ','.join(pyfiles)
#             submit_args = '{} --py-files {}'.format(submit_args, submit_pyfiles)
#
#         submit_args = '{} pyspark-shell'.format(submit_args)
#
#         # os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.postgresql:postgresql:42.2.5 pyspark-shell"
#         os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
#         print('PYSPARK_SUBMIT_ARGS: {}'.format(submit_args))
#
#         conf = SparkConf()
#
#         # jobname
#         project_settings = project.config()
#         repo_name = project_settings['repository']['name']
#         default_jobname = project_settings['profile'] + repo_name
#         jobname = config.get('jobname', default_jobname)
#         conf.setAppName(jobname)
#
#         md = metadata.config()
#         for v in md['providers'].values():
#             if v['service'] == 'minio':
#                 conf.set("spark.hadoop.fs.s3a.endpoint", 'http://{}:{}'.format(v['hostname'], v.get('port', 9000))) \
#                     .set("spark.hadoop.fs.s3a.access.key", v['access']) \
#                     .set("spark.hadoop.fs.s3a.secret.key", v['secret']) \
#                     .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#                     .set("spark.hadoop.fs.s3a.path.style.access", True)
#                 break
#
#         # set master
#         conf.setMaster(config.get('master', 'local[*]'))
#
#         # set log level fro spark
#         sc = SparkContext(conf=conf)
#
#         # pyspark set log level method
#         # (this will not suppress WARN before starting the context)
#         sc.setLogLevel("ERROR")
#
#         self._ctx = SQLContext(sc)
#         self._info = {'name': name, 'context': 'spark', 'metadata': config, 'conf': dict(conf.getAll())}
#
#         self.logger = logging.get()
#
#     def config(self):
#         return ImmutableDict(self._info)
#
#     def context(self):
#         return self._ctx
#
#     def read(self, resource=None, provider=None, md=dict(), **kargs):
#         md = metadata.resource(resource, provider, md)
#
#         if not md:
#             self.logger.error("No metadata")
#             return None
#
#         obj = self._read(md, **kargs)
#
#         # logging
#         logdata = {
#             'format': md['format'],
#             'service': md['service'],
#             'path': md['resource_path'],
#             'url': md['url']}
#
#         logtype = {'dlf_type': '{}.{}'.format(self.__class__.__name__, func_name())}
#         self.logger.info(logdata, extra=logtype)
#         return obj
#
#     def _read(self, md, **kargs):
#         obj = self.load_with_schema(md, **kargs)
#         obj = self._filter(obj, md, 'read')
#         obj = self._cache(obj, md, 'read')
#         obj = self._transform(obj, md, 'read')
#
#         reserved_cols = ['_updated', '_date', '_datetime', '_state']
#
#         return obj.drop(*reserved_cols) if obj else None
#
#     def _cache(self, df, repartition=None, access):
#
#         repartition = md[access]['partition'].get('repartition')
#         coalesce = md[access]['partition'].get('coalesce')
#         cache = md[access]['partition'].get('cache')
#
#         df = df.repartition(repartition) if repartition else df
#         df = df.coalesce(coalesce) if coalesce else df
#         df = df.cache() if cache else df
#
#         return df
#
#     def _filter(self, df, md, access):
#         df =filter_transform(df, md, access)
#         return df
#
#     def _transform(self, df, md, access):
#
#         mapping = md[access]['mapping']
#         df = mapping_transform(df, mapping) if mapping else df
#
#         return df
#
#     def load_schema(self, md):
#
#         # schema table: id, datetime, json
#         schema_path = '{}/schema'.format(md['resource_path'])
#         df_schema = self.load(merge(md, {'resource_path': schema_path}), _logging=False)
#
#         if df_schema:
#             return df_schema.sort('ts').limit(1).collect()[0].asDict()
#         else:
#             return None
#
#     def save_schema(self, obj, md, mode='append'):
#         # constants:
#         reserved_cols = ['_updated', '_date', '_datetime', '_state']
#
#         # object schema
#         cols = set(obj.columns).difference(set(reserved_cols))
#         ordered_colnames = [x for x in obj.columns if x in cols]
#         obj_schema = obj[ordered_colnames].schema.json()
#
#         # default is schema has changed
#         same_schema = False
#
#         if mode=='append':
#             #get last schema
#             schema = self.load_schema(md)
#
#             # has the schema changed since the last write?
#             same_schema = schema and obj_schema == schema.get('json','')
#
#         # append/write the new schema, if not the same schema
#         if not same_schema:
#             # default
#             now = datetime.now()
#
#             # Different schema, update schema table with new entry
#             schema_entry = (now.strftime('%Y%m%dT%H%M%S%f'), now, obj_schema)
#             df_schema = self.context().createDataFrame([schema_entry], ['id', 'ts', 'json'])
#
#             # write the schema to destination provider
#             schema_path = '{}/schema'.format(md['resource_path'])
#             self.save(df_schema, merge(md, {'resource_path': schema_path}), mode=mode)
#
#
#     def load_with_schema(self, md, **kargs):
#         return self.load(md, **kargs)
#
#     def load(self, md, _logging=True, **kargs):
#         obj = None
#
#         options = merge(md['read']['options'], kargs)
#
#         try:
#             if md['service'] in ['local', 'hdfs', 'minio']:
#                 if md['format'] == 'csv':
#                     obj = self._ctx.read.csv(md['url'], **options)
#                 if md['format'] == 'json':
#                     obj = self._ctx.read.option('multiLine', True).json(md['url'], **options)
#                 if md['format'] == 'jsonl':
#                     obj = self._ctx.read.json(md['url'], **options)
#                 elif md['format'] == 'parquet':
#                     obj = self._ctx.read.parquet(md['url'], **options)
#             elif md['service'] in ['sqlite', 'mysql', 'postgres', 'mssql', 'oracle']:
#                 obj = self._ctx.read \
#                     .format('jdbc') \
#                     .option('url', md['url']) \
#                     .option("dbtable", md['resource_path']) \
#                     .option("driver", md['driver']) \
#                     .option("user", md['username']) \
#                     .option('password', md['password']) \
#                     .load(**options)
#             elif md['service'] == 'elastic':
#                 results = elastic_read(md['url'], options.get('query', {}))
#                 rows = [pyspark.sql.Row(**r) for r in results]
#                 obj =  self.context().createDataFrame(rows)
#             else:
#                 raise ValueError(f'Unknown service "{md["service"]}"')
#         except Exception as e:
#             if _logging:
#                 self.logger.error('could not load')
#                 print(e)
#             return None
#
#         return obj
#
#     def write(self, obj, resource=None, provider=None, **kargs):
#         md = metadata.resource(resource, provider)
#         if not md:
#             self.logger.exception("No metadata")
#             return None
#
#         self._write(obj, md, **kargs)
#
#         logdata = {
#             'format': md['format'],
#             'service': md['service'],
#             'path': md['resource_path'],
#             'url': md['url']}
#
#         logtype = {'dlf_type': '{}.{}'.format(self.__class__.__name__, func_name())}
#         self.logger.info(logdata, extra=logtype)
#
#     def _write(self, obj, md, **kargs):
#         df = self._transform(obj, md, 'write')
#         df = self._filter(df, md, 'write')
#         df = self._cache(df, md, 'write')
#
#         # override options on provider with options on resource, with option on the read method
#         df = sparkfun.filter.add_reserved_columns(df, md['write'])
#
#         # define partition structure as _date / user_defined_partition / _updated
#         kargs['partitionBy'] = ['_date'] + kargs.get('partitionBy', []) + ['_updated']
#         self.save_with_schema(df, md, **kargs)
#
#     def save_with_schema(self, obj, md, schema_save_mode='append', **kargs):
#         self.save_schema(obj, md, mode=schema_save_mode)
#
#         schema = self.load_schema(md)
#         resource_path = md['resource_path']
#         resource_path += f'/{schema["id"]}' if schema else ''
#
#         self.save(obj, merge(md, {'resource_path': resource_path}), **kargs)
#
#     def save(self, obj, md, _logging=True, **kargs):
#         options = merge(md['read']['options'], kargs)
#
#         try:
#             if md['service'] in ['local', 'hdfs', 'minio']:
#                 if md['format'] == 'csv':
#                     obj.write.csv(md['url'], **options)
#                 if md['format'] == 'json':
#                     obj.write.option('multiLine', True).json(md['url'], **options)
#                 if md['format'] == 'jsonl':
#                     obj.write.json(md['url'], **options)
#                 elif md['format'] == 'parquet':
#                     obj.write.parquet(md['url'], **options)
#                 else:
#                     self.logger.info('format unknown')
#
#             elif md['service'] in ['sqlite', 'mysql', 'postgres', 'oracle']:
#                 obj.write \
#                     .format('jdbc') \
#                     .option('url', md['url']) \
#                     .option("dbtable", md['resource_path']) \
#                     .option("driver", md['driver']) \
#                     .option("user", md['username']) \
#                     .option('password', md['password']) \
#                     .save(**options)
#             elif md['service'] == 'elastic':
#                 mode = kargs.get("mode", None)
#                 elastic_write(obj, md['url'], mode, md['resource_path'], options['settings'], options['mappings'])
#             else:
#                 raise ValueError('downt know how to handle this')
#         except Exception as e:
#             if _logging:
#                 self.logger.error('could not save')
#                 print(e)
#
#
#     def ingest(self,src_resource=None, src_provider=None, trg_resource=None, trg_provider=None, **kargs):
#
#         # source metadata
#         md_src = metadata.resource(src_resource, src_provider)
#
#         # default path for destination is src path
#         if (not trg_resource)  and trg_provider:
#             trg_resource = src_resource
#
#         #### Metadata
#         md_trg = project.resource(trg_resource, trg_provider)
#
#         df_diff = self._ingest(md_src, md_trg, **kargs)
#
#         # some statistics
#         records_add = df_diff.filter("_state = 0").count()
#         records_del = df_diff.filter("_state = 1").count()
#
#         logdata = {
#             'src_url': md_src['url'],
#             'trg_url': md_trg['url'],
#             'upserts': records_add,
#             'deletes': records_del}
#
#         logtype = {'dlf_type': '{}.{}'.format(self.__class__.__name__, func_name())}
#         self.logger.info(logdata, extra=logtype)
#
#     def _ingest(self, md_src, md_trg, **kargs):
#
#         # filter settings from src (provider and resource)
#         filter_params = merge(
#             md_src['provider'].get('read', {}).get('filter', {}),
#             md_src['resource'].get('read', {}).get('filter', {}))
#
#         #### Target match filter params:
#         if 'read' not in md_trg['resource']:
#             md_trg['resource']['read'] = {}
#
#         # match filter with the one from source resource
#         md_trg['resource']['read']['filter'] = filter_params
#
#         #### Read src/trg resources
#         df_src = self._read(md_src, _logging=False)
#
#         #### Get target write options
#         # override options on provider with options on resource, with option on the ingest method
#         options = merge(md_trg['provider'].get('write', {}).get('options', {}),
#                               md_trg['resource'].get('write', {}).get('options', {}))
#         options = merge(options, kargs)
#
#         # compare if mode!=overwrite
#         df_trg = self._read(md_trg, _logging=False) if options.get('mode')!='overwrite' else None
#
#         # compare
#         df_diff = dataframe_update(df_src, df_trg)
#
#         if df_diff.count():
#             # print('partitions:', partitions)
#             # df_diff.show() # todo remove
#             # print(md_trg)  # todo remove
#             self._write(df_diff, md_trg, _logging=True, **options)
#
#         # print("returned dataframe")
#         # df_diff.show()  # todo remove
#         return df_diff
