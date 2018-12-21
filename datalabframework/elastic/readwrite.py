import elasticsearch
from collections import deque

def read(url, query):
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


def write(obj, uri, mode='append', index_name=None, settings=None, mappings=None):
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

    deque(elasticsearch.helpers.parallel_bulk(es, obj, index=index_name, doc_type=mappings["doc_type"]), maxlen=0)
    es.indices.refresh()
