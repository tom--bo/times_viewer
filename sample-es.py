import sys
import yaml
import datetime
import pytz
from elasticsearch import Elasticsearch
from mysql import MySQL


es = Elasticsearch()
index = "times"

def initialize_es():
    es = Elasticsearch()
    mapping = {
        "properties": {
            "id": { "type": "long"},
            "elapsed_time": { "type": "long"},
            "user": { "index": "not_analyzed", "type": "string" },
            "task": { "index": "not_analyzed", "type": "string" },
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

    es.indices.create(index=index)
    es.indices.put_mapping(index=index, doc_type='mydoc', body=mapping)


def add_tasks(tasks):
    data = []
    jst = pytz.timezone('Asia/Tokyo')

    for task in tasks:
        delta = task[4] - task[3]
        doc = {
            'id':           task[0],
            'user':         task[1],
            'task':         task[2],
            'begin':        jst.localize(task[3]).strftime('%Y-%m-%d %H:%M:%S'),
            'finish':       jst.localize(task[4]).strftime('%Y-%m-%d %H:%M:%S'),
            'elapsed_time': delta.total_seconds(),
            'created_at':   jst.localize(task[5]).strftime('%Y-%m-%d %H:%M:%S')
        }
        res = es.index(index=index, doc_type='mydoc', body=doc)

def get_latest_datetime():
    res = es.search(index=index, doc_type='mydoc', sort=["begin:desc"])
    # for r in res['hits']['hits']:
    #     print(r['_source']['begin'])
    return res['hits']['hits'][0]['_source']['begin']

 
if __name__ == '__main__':
    init_flag = False 
    db = MySQL(0)
    if init_flag:
        initialize_es()
        ret = db.get_task_list()
    else:
        latest_time = get_latest_datetime()
        ret = db.get_latest_task_list(latest_time)

    add_tasks(ret)

