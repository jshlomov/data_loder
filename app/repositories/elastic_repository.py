from pprint import pprint
from typing import List

import elasticsearch
from elasticsearch.helpers import bulk
from pandas import DataFrame

from app.db.elastic_database import get_elasticsearch_client


def insert_all_data_elastic(data : List[dict]):
    es_client = get_elasticsearch_client()
    try:
        bulk(es_client, data)
        print("Successfully inserted data into Elasticsearch")
    except elasticsearch.helpers.BulkIndexError as e:
        pprint(e.errors)
    es_client.close()