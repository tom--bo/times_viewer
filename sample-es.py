import sys
import yaml
import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch()

mapping = {
    "mydoc": {
        "properties": {
            "user": { "type": "string" },
            "task": { "type": "string" },
            "begin": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
            },
            "finish": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
            },
            "created_at": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
            },
        }
    }
}

es.indices.create(index='times-view')
es.indices.put_mapping(index='times-view', doc_type='mydoc', body=mapping)


doc = {
    'user': 'kimchy',
    'task': 'Elasticsearch: cool. bonsai cool.',
    'begin': '2017-03-22 10:00:00',
    'finish': '2017-03-22 12:00:00',
    'created_at': '2017-10-09 17:15:00'
}

res = es.index(id=1, index="times-view", doc_type='mydoc', body=doc)

