import sys
import yaml
import datetime
from elasticsearch import Elasticsearch
from mysql import MySQL


es = Elasticsearch()

def initialize_es():
    es = Elasticsearch()
    mapping = {
        "mydoc": {
            "properties": {
                "id": { "type": "long" },
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


def add_tasks(tasks):
    data = []

    for task in tasks:
        doc = {
            'id':         task[0],
            'user':       task[1],
            'task':       task[2],
            'begin':      task[3].strftime('%Y-%m-%d %H:%M:%S'),
            'finish':     task[4].strftime('%Y-%m-%d %H:%M:%S'),
            'created_at': task[5].strftime('%Y-%m-%d %H:%M:%S')
        }
        res = es.index(index="times-view", doc_type='mydoc', body=doc)

def get_latest():
    #res = es.get(index="times-view", doc_type='mydoc', id=1)
    return res

 
if __name__ == '__main__':
    initialize_es()
    db = MySQL(0)
    ret = db.get_task_list()

    add_tasks(ret)

