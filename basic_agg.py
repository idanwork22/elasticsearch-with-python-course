from elasticsearch import Elasticsearch
from pprint import pprint

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

    # Aggregating on the team field to get the number of players on each team:
    query = {
        "size": 0,
        "aggs": {
            "team_count": {
                "terms": {
                    "field": "team"
                }
            }
        }
    }
    # execute the search and print the results
    response = es.search(index=INDEX_NAME, body=query)

    # get only the documents
    for hit in response['aggregations']['team_count']['buckets']:
        print(hit)


if __name__ == '__main__':
    main()
