from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

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

    s = Search(using=es, index=INDEX_NAME) \
        .query("match", team="Nets")

    # execute the search and print the results
    response = s.execute()

    # see full response from the server
    print(f"response: \n {response}")

    for hit in response:
        print(hit.first_name, hit.last_name)


if __name__ == '__main__':
    main()
