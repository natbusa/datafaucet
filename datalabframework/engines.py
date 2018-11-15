import os

from datalabframework.spark.mapping import transform as mapping_transform
from datalabframework.spark.filter import transform as filter_transform
from datalabframework.spark.diff import dataframe_update

from . import project
from . import params
from . import data
from . import utils
from . import spark as sparkfun

from copy import deepcopy

import elasticsearch.helpers

from datetime import datetime

import pyspark
from pyspark.sql.functions import desc, date_format, to_utc_timestamp, to_timestamp

from . import logging

# purpose of engines
# abstract engine init, data read and data write
# and move this information to metadata

# it does not make the code fully engine agnostic though.

engines = dict()

import sys

def func_name():
    # noinspection PyProtectedMember
    return sys._getframe(1).f_code.co_name

class SparkEngine:
    def __init__(self, name, config):
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SQLContext

        submit_args = ''

        jars = []
        jars += config.get('jars', [])
        if jars:
            submit_jars = ' '.join(jars)
            submit_args = '{} --jars {}'.format(submit_args, submit_jars)

        packages = config.get('packages', [])
        if packages:
            submit_packages = ','.join(packages)
            submit_args = '{} --packages {}'.format(submit_args, submit_packages)

        pyfiles = config.get('py-files', [])
        if pyfiles:
            submit_pyfiles = ','.join(pyfiles)
            submit_args = '{} --py-files {}'.format(submit_args, submit_pyfiles)

        submit_args = '{} pyspark-shell'.format(submit_args)

        # os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.postgresql:postgresql:42.2.5 pyspark-shell"
        os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
        print('PYSPARK_SUBMIT_ARGS: {}'.format(submit_args))

        conf = SparkConf()

        # jobname
        default_jobname = project.profile()
        default_jobname += utils.repo_data()['name'] if utils.repo_data()['name'] else ''
        jobname = config.get('jobname', default_jobname)
        conf.setAppName(jobname)

        rmd = params.metadata()['providers']
        for v in rmd.values():
            if v['service'] == 'minio':
                conf.set("spark.hadoop.fs.s3a.endpoint", 'http://{}:{}'.format(v['hostname'], v.get('port', 9000))) \
                    .set("spark.hadoop.fs.s3a.access.key", v['access']) \
                    .set("spark.hadoop.fs.s3a.secret.key", v['secret']) \
                    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .set("spark.hadoop.fs.s3a.path.style.access", True)
                break

        # set master
        conf.setMaster(config.get('master', 'local[*]'))

        # set log level fro spark
        sc = SparkContext(conf=conf)

        # pyspark set log level method
        # (this will not suppress WARN before starting the context)
        sc.setLogLevel("ERROR")

        self._ctx = SQLContext(sc)
        self.info = {'name': name, 'context': 'spark', 'metadata': config, 'conf': conf.getAll()}

        self.logger = logging.getLogger()


    def context(self):
        return self._ctx

    def read(self, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)

        if not md:
            logger.error("No metadata")
            return None

        obj = self._read(md, **kargs)

        # logging
        logdata = {
            'format': md['provider']['format'],
            'service': md['provider']['service'],
            'path': md['resource']['path'],
            'url': md['url']}

        logtype = {'dlf_type': '{}.{}'.format(self.__class__.__name__, func_name())}
        self.logger.info(logdata, extra=logtype)
        return obj

    def _read(self, md, **kargs):
        obj = self.load_with_schema(md, **kargs)
        obj = self._filter(obj, md, 'read')
        obj = self._cache(obj, md, 'read')
        obj = self._transform(obj, md, 'read')

        reserved_cols = ['_updated', '_date', '_datetime', '_state']

        return obj.drop(*reserved_cols) if obj else None

    def _cache(self, obj, md, access):

        pmd = md['provider']
        rmd = md['resource']

        repartition = utils.merge(pmd.get(access, {}).get('repartition', {}),
                             rmd.get(access, {}).get('repartition',{}))

        coalesce = utils.merge(pmd.get(access, {}).get('coalesce', {}),
                             rmd.get(access, {}).get('coalesce',{}))

        cache = utils.merge(pmd.get(access, {}).get('cache', {}),
                             rmd.get(access, {}).get('cache',{}))

        df = obj

        df = df.repartition(repartition) if repartition else df
        df = df.coalesce(coalesce) if coalesce else df
        df = df.cache() if cache else df

        return df

    def _filter(self, obj, md, access):
        pmd = md['provider']
        rmd = md['resource']

        params = dict()
        params['read'] = utils.merge(pmd.get('read', {}), rmd.get('read', {}))
        params['write'] = utils.merge(pmd.get('write', {}), rmd.get('write', {}))

        df =filter_transform(obj, params, access) if params else obj
        return df

    def _transform(self, obj, md, access):

        pmd = md['provider']
        rmd = md['resource']

        mapping = pmd.get(access, {}).get('mapping', None)
        mapping = rmd.get(access, {}).get('mapping', mapping)

        df = mapping_transform(obj, mapping) if mapping else obj

        return df

    def load_schema(self, md):

        # schema table: id, datetime, json
        schema_path = '{}/schema'.format(md['resource']['path'])
        df_schema = self.load(data.metadata(path=schema_path, provider=md['resource']['provider']), _logging=False)

        if df_schema:
            return df_schema.sort(desc('ts')).limit(1).collect()[0].asDict()
        else:
            return dict()

    def save_schema(self, obj, md, mode='append'):
        # constants:
        reserved_cols = ['_updated', '_date', '_datetime', '_state']

        # object schema
        cols = set(obj.columns).difference(set(reserved_cols))
        ordered_colnames = [x for x in obj.columns if x in cols]
        obj_schema = obj[ordered_colnames].schema.json()

        # default is schema has changed
        same_schema = False

        if mode=='append':
            #get last schema
            schema = self.load_schema(md)

            # has the schema changed since the last write?
            same_schema = schema and obj_schema == schema.get('json','')

        # append/write the new schema, if not the same schema
        if not same_schema:
            # default
            now = datetime.now()

            # Different schema, update schema table with new entry
            schema_entry = (now.strftime('%Y%m%dT%H%M%S%f'), now, obj_schema)
            df_schema = self.context().createDataFrame([schema_entry], ['id', 'ts', 'json'])

            # write the schema to destination provider
            schema_path = '{}/schema'.format(md['resource']['path'])
            md = data.metadata(path=schema_path, provider=md['resource']['provider'])
            self.save(df_schema, md, mode=mode)


    def load_with_schema(self, md, **kargs):
        schema = self.load_schema(md)

        resource_path = md['resource']['path']
        resource_path += f'/{schema["id"]}' if schema else ''

        # path -updated with appended schema if available
        resource_md = deepcopy(md)
        resource_md['resource']['path'] = resource_path
        resource_md['url'] = data._url(resource_md)

        return self.load(resource_md, **kargs)

    def load(self, md, _logging=True, **kargs):

        pmd = md['provider']
        rmd = md['resource']

        url = md['url']

        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pmd.get('read', {}).get('options', {}), rmd.get('read', {}).get('options', {}))
        options = utils.merge(options, kargs)

        obj = None

        try:
            if pmd['service'] in ['local', 'hdfs', 'minio']:
                if pmd['format'] == 'csv':
                    obj = self._ctx.read.csv(url, **options)
                if pmd['format'] == 'json':
                    obj = self._ctx.read.option('multiLine', True).json(url, **options)
                if pmd['format'] == 'jsonl':
                    obj = self._ctx.read.json(url, **options)
                elif pmd['format'] == 'parquet':
                    obj = self._ctx.read.parquet(url, **options)
            elif pmd['service'] == 'sqlite':
                driver = "org.sqlite.JDBC"
                obj = self._ctx.read \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .load(**options)
            elif pmd['service'] == 'mysql':
                driver = "com.mysql.cj.jdbc.Driver"
                obj = self._ctx.read \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .option("user", pmd['username']) \
                    .option('password', pmd['password']) \
                    .load(**options)
            elif pmd['service'] == 'postgres':
                driver = "org.postgresql.Driver"
                obj = self._ctx.read \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .option("user", pmd['username']) \
                    .option('password', pmd['password']) \
                    .load(**options)
            elif pmd['service'] == 'mssql':
                driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                obj = self._ctx.read \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .option("user", pmd['username']) \
                    .option('password', pmd['password']) \
                    .load(**options)
            elif pmd['service'] == 'oracle':
                driver = "oracle.jdbc.driver.OracleDriver"
                obj = self._ctx.read \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .load(**options)
            elif pmd['service'] == 'elastic':
                results = elastic_read(url, options.get('query', {}))
                rows = [pyspark.sql.Row(**r) for r in results]
                obj =  self.context().createDataFrame(rows)

            else:
                raise ValueError(f'Unknown service provider "{pmd["service"]}"')
        except Exception as e:
            if _logging:
                self.logger.error('could not load')
                print(e)
            return None

        return obj


    def write(self, obj, resource=None, path=None, provider=None, **kargs):
        md = data.metadata(resource, path, provider)
        if not md:
            logger.exception("No metadata")
            return None

        self._write(obj, md, **kargs)

        logdata = {
            'format': md['provider']['format'],
            'service': md['provider']['service'],
            'path': md['resource']['path'],
            'url': md['url']}

        logtype = {'dlf_type': '{}.{}'.format(self.__class__.__name__, func_name())}
        self.logger.info(logdata, extra=logtype)

    def _write(self, obj, md, **kargs):
        df = self._transform(obj, md, 'write')
        df = self._filter(df, md, 'write')
        df = self._cache(df, md, 'write')

        # add reserved columns
        pmd = md['provider']
        rmd = md['resource']

        # override options on provider with options on resource, with option on the read method
        write_options = utils.merge(pmd.get('write', {}), rmd.get('write', {}))
        df = sparkfun.filter.add_reserved_columns(df, write_options)

        # define partition structure as _date / user_defined_partition / _updated
        kargs['partitionBy'] = ['_date'] + kargs.get('partitionBy', []) + ['_updated']

        self.save_with_schema(df, md, **kargs)

    def save_with_schema(self, obj, md, schema_save_mode='append', **kargs):
        self.save_schema(obj, md, mode=schema_save_mode)

        schema = self.load_schema(md)
        resource_path = md['resource']['path']
        resource_path += f'/{schema["id"]}' if schema else ''

        # path -updated with appended schema if available
        resource_md = deepcopy(md)
        resource_md['resource']['path'] = resource_path
        resource_md['url'] = data._url(resource_md)

        self.save(obj, resource_md, **kargs)

    def save(self, obj, md, _logging=True, **kargs):
        pmd = md['provider']
        rmd = md['resource']

        url = md['url']

        # override options on provider with options on resource, with option on the read method
        options = utils.merge(pmd.get('write', {}).get('options', {}), rmd.get('write', {}).get('options', {}))
        options = utils.merge(options, kargs)

        try:
            if pmd['service'] in ['local', 'hdfs', 'minio']:
                if pmd['format'] == 'csv':
                    obj.write.csv(url, **options)
                if pmd['format'] == 'json':
                    obj.write.option('multiLine', True).json(url, **options)
                if pmd['format'] == 'jsonl':
                    obj.write.json(url, **options)
                elif pmd['format'] == 'parquet':
                    obj.write.parquet(url, **options)
                else:
                    logger.info('format unknown')

            elif pmd['service'] == 'sqlite':
                driver = "org.sqlite.JDBC"
                obj.write \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .save(**kargs)

            elif pmd['service'] == 'mysql':
                driver = "com.mysql.cj.jdbc.Driver"
                obj.write \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .option("user", pmd['username']) \
                    .option('password', pmd['password']) \
                    .save(**kargs)

            elif pmd['service'] == 'postgres':
                driver = "org.postgresql.Driver"
                obj.write \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .option("user", pmd['username']) \
                    .option('password', pmd['password']) \
                    .save(**kargs)
            elif pmd['service'] == 'oracle':
                driver = "oracle.jdbc.driver.OracleDriver"
                obj.write \
                    .format('jdbc') \
                    .option('url', url) \
                    .option("dbtable", rmd['path']) \
                    .option("driver", driver) \
                    .save(**kargs)
            elif pmd['service'] == 'elastic':
                uri = 'http://{}:{}'.format(pmd["hostname"], pmd["port"])
                mode = kargs.get("mode", None)
                elastic_write(obj, uri, mode, rmd["path"], options["settings"], options["mappings"])
            else:
                raise ValueError('downt know how to handle this')
        except Exception as e:
            if _logging:
                self.logger.error('could not save')
                print(e)


    def ingest(self,
               src_resource=None, src_path=None, src_provider=None,
               trg_resource=None, trg_path=None, trg_provider=None, **kargs):

        # source metadata
        md_src = data.metadata(src_resource, src_path, src_provider)

        # default path for destination is src path
        if (not trg_resource) and (not trg_path) and trg_provider:
            trg_path = md_src['resource']['path']

        # default provider for destination is src provider
        if (not trg_resource) and (not trg_provider) and trg_path and trg_path != src_path:
            trg_provider = md_src['resource']['provider']

        #### Metadata
        md_trg = data.metadata(trg_resource, trg_path, trg_provider)

        df_diff = self._ingest(md_src, md_trg, **kargs)

        # some statistics
        records_add = df_diff.filter("_state = 0").count()
        records_del = df_diff.filter("_state = 1").count()

        logdata = {
            'src_url': md_src['url'],
            'trg_url': md_trg['url'],
            'upserts': records_add,
            'deletes': records_del}

        logtype = {'dlf_type': '{}.{}'.format(self.__class__.__name__, func_name())}
        self.logger.info(logdata, extra=logtype)

    def _ingest(self, md_src, md_trg, **kargs):

        # filter settings from src (provider and resource)
        filter_params = utils.merge(
            md_src['provider'].get('read', {}).get('filter', {}),
            md_src['resource'].get('read', {}).get('filter', {}))

        #### Target match filter params:
        if 'read' not in md_trg['resource']:
            md_trg['resource']['read'] = {}

        # match filter with the one from source resource
        md_trg['resource']['read']['filter'] = filter_params

        #### Read src/trg resources
        df_src = self._read(md_src, _logging=False)

        #### Get target write options
        # override options on provider with options on resource, with option on the ingest method
        options = utils.merge(md_trg['provider'].get('write', {}).get('options', {}),
                              md_trg['resource'].get('write', {}).get('options', {}))
        options = utils.merge(options, kargs)

        # compare if mode!=overwrite
        df_trg = self._read(md_trg, _logging=False) if options.get('mode')!='overwrite' else None

        # compare
        df_diff = dataframe_update(df_src, df_trg)

        if df_diff.count():
            # print('partitions:', partitions)
            # df_diff.show() # todo remove
            # print(md_trg)  # todo remove
            self._write(df_diff, md_trg, _logging=True, **options)

        # print("returned dataframe")
        # df_diff.show()  # todo remove
        return df_diff

def elastic_read(url, query):
    """
    :param url:
    :param query:
    :return: python list of dict

    sample resource:
    variables:
        size: 10
        from: 0
        query: "laptop"
    keywords_search:
        provider: elastic_test
        path: /search_keywords_dev/keyword/
        query: >
            {
              "size" : "{{ default.variables.size }}",
              "from" : "{{ default.variables.size }}",
              "query": {
                "function_score": {
                  "query": {
                      "match" : {
                        "query_wo_tones": {"query":"{{ default.variables.query }}", "operator" :"or", "fuzziness":"AUTO"}
                    }
                  },
                  "script_score" : {
                      "script" : {
                        "source": "Math.sqrt(doc['impressions'].value)"
                      }
                  },
                  "boost_mode":"multiply"
                }
              }
            }
    """

    try:
        es = elasticsearch.Elasticsearch([url])
        res = es.search(body=query)
    except:
        raise ValueError("Cannot query elastic search")

    hits = res.pop("hits", None)
    if hits is None:
        # handle ERROR
        raise ValueError("Query failed")

        # res["total_hits"] = hits["total"]
        # res["max_score"] = hits["max_score"]
        # print("Summary:", res)

    hits2 = []
    for hit in hits['hits']:
        hitresult = hit.pop("_source", None)
        hits2.append({**hit, **hitresult})

    return hits2


def elastic_write(obj, uri, mode='append', index_name=None, settings=None, mappings=None):
    """
    :param mode: overwrite | append
    :param obj: spark dataframe, or list of Python dictionaries
    :return:
    """
    es = elasticsearch.Elasticsearch([uri])
    if mode == "overwrite":
        # properties:
        #                 {
        #                     "count": {
        #                         "type": "integer"
        #                     },
        #                     "keyword": {
        #                         "type": "keyword"
        #                     },
        #                     "query": {
        #                         "type": "text",
        #                         "fields": {
        #                             "keyword": {
        #                                 "type": "keyword",
        #                                 "ignore_above": 256
        #                             }
        #                         }
        #                     }
        #                 }
        # OR:
        # properties:
        #     count: integer
        #     keyword: keyword
        #     query:
        #         type: text
        #         fields:
        #             keyword:
        #                 type: keyword
        #                 ignore_above: 256
        #     query_wo_tones:
        #         type: text
        #         fields:
        #             keyword:
        #                 type: keyword
        #                 ignore_above: 256
        for k, v in mappings["properties"].items():
            if isinstance(mappings["properties"][k], str):
                mappings["properties"][k] = {"type": mappings["properties"][k]}
            # settings:
            #         {
            #             "index": {
            #                 "number_of_shards": 1,
            #                 "number_of_replicas": 3,
            #                 "mapping": {
            #                     "total_fields": {
            #                         "limit": "1024"
            #                     }
            #                 }
            #             }
            #         }
            # OR:
            # settings:
            #     index:
            #         number_of_shards: 1
            #         number_of_replicas: 3
            #         mapping:
            #             total_fields:
            #                 limit: 1024


        if not settings or not settings:
            raise ValueError("'settings' and 'mappings' are required for 'overwrite' mode!")
        es.indices.delete(index=index_name, ignore=404)
        es.indices.create(index=index_name, body={
            "settings": settings,
            "mappings": {
                mappings["doc_type"]: {
                    "properties": mappings["properties"]
                }
            }
        })
    elif mode == "append":  # append
        pass
    else:
        raise ValueError("Unsupported mode: " + mode)

    if isinstance(obj, pyspark.sql.dataframe.DataFrame):
        obj = [row.asDict() for row in obj.collect()]
    else:  # python list of python dictionaries
        pass

    from collections import deque
    deque(elasticsearch.helpers.parallel_bulk(es, obj, index=index_name, doc_type=mappings["doc_type"]), maxlen=0)
    es.indices.refresh()


def get(name):
    global engines

    # get
    engine = engines.get(name)

    if not engine:
        # create
        md = params.metadata()
        cn = md['engines'].get(name)
        config = cn.get('config', {})

        if cn['context'] == 'spark':
            engine = SparkEngine(name, config)
            engines[name] = engine

    return engine
