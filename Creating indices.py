from elasticsearch import Elasticsearch

ELASTIC_PASSWORD = "6fX0s0E23oIeF4BGkYY9"
ELASTIC_USERNAME = "elastic"
ELASTIC_PATH = "http://localhost:9200"
INDEX_NAME = "nba_players"
mapping = {
    "first_name": {
        "type": "text"
    },
    "last_name": {
        "type": "text"
    },
    "date_of_birth": {
        "type": "date"
    },
    "position": {
        "type": "keyword"
    },
    "team": {
        "type": "keyword"
    },
    "avg_scoring": {
        "type": "float"
    },
    "avg_rebound": {
        "type": "float"
    },
    "avg_assist": {
        "type": "float"
    },
    "country": {
        "type": "keyword"
    }}


def connect_to_elastic() -> Elasticsearch:
    client = Elasticsearch(
        ELASTIC_PATH,
        verify_certs=False,
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
    )
    return client


def main():
    client = connect_to_elastic()
    client.indices.create(index=INDEX_NAME,
                          body={
                              'mappings': {
                                  'properties': mapping
                              }
                          })


if __name__ == '__main__':
    main()
