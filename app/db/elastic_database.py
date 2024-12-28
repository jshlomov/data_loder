from elasticsearch import Elasticsearch

def get_elasticsearch_client():
    client = Elasticsearch(
        hosts=['http://localhost:9200'],
        basic_auth=("elastic", "123456"),
        verify_certs=False
    )
    return client


def create_index(index_name, es_client):
    if es_client.indices.exists(index=index_name):
        print(f"Deleting existing index: {index_name}")
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
    print(f"Index '{index_name}' created successfully.")


def init_elastic():
    try:
        es_client = get_elasticsearch_client()
        create_index("attacks", es_client)
    except Exception as e:
        print(f"Error initializing Elasticsearch: {e}")

if __name__ == '__main__':
    init_elastic()