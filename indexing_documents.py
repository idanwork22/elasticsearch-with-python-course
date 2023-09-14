from elasticsearch import Elasticsearch
from list_of_documents import list_of_nba_players

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
    client = connect_to_elastic()
    for id_for_elastic, document in enumerate(list_of_nba_players):
        client.index(index=INDEX_NAME, body=document, id=id_for_elastic)


if __name__ == '__main__':
    main()
