from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

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

    query = {
        "query": {
            "match_all": {}
        }
    }

    page_size = 1000  # number of results per page

    # use the scan() helper to efficiently retrieve large numbers of search results
    scroll = scan(client=es, query=query, index=INDEX_NAME, scroll='2m', size=page_size)

    # loop over the search results and print the 'full_name' field of each hit
    for hit in scroll:
        print(hit['_source'])


if __name__ == '__main__':
    main()
