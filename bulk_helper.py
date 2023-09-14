from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

ELASTIC_PASSWORD = "6fX0s0E23oIeF4BGkYY9"
ELASTIC_USERNAME = "elastic"
ELASTIC_PATH = "http://localhost:9200"
INDEX_NAME = "nba_players"


def connect_to_elastic() -> Elasticsearch:
    client = Elasticsearch(
        ELASTIC_PATH,
        verify_certs=False,
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
    )
    return client


def main():
    es = connect_to_elastic()

    # define the documents to be inserted

    actions = [
        {
            '_index': INDEX_NAME,
            '_source': {
                "first_name": "Kobe",
                "last_name": "Bryant",
                "team": "Lakers",
                "position": "Guard",
                "avg_scoring": 25.0,
                "avg_rebound": 5.2,
                "avg_assist": 4.7,
                "date_of_birth": "1978-08-23"
            }
        },
        {
            '_index': INDEX_NAME,
            '_source': {
                "first_name": "Michael",
                "last_name": "Jordan",
                "team": "Bulls",
                "position": "Guard",
                "avg_scoring": 30.1,
                "avg_rebound": 6.2,
                "avg_assist": 5.3,
                "date_of_birth": "1963-02-17"

            }
        }
    ]

    # use the bulk() helper to insert the documents in bulk
    response = bulk(client=es, actions=actions)
    print(response)


if __name__ == '__main__':
    main()
