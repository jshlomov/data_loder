from elasticsearch import Elasticsearch



def get_elasticsearch_client():
    client = Elasticsearch(
        ['http://localhost:9200'],
        basic_auth=("elastic", "3uDiv6AS"),
        verify_certs=False
    )
    return client


def create_index(index_name, es_client):
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
    es_client.indices.create(index=index_name, body={
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 2
        },
        "mappings": {
            "properties": {
                "summary": {"type": "text"},
                "date": {"type": "date"}
            }
        }
    })


def init_elastic():
    es_client = get_elasticsearch_client()
    create_index("attacks", es_client)