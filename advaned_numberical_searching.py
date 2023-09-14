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
    # create a query dict that matches documents where the sum of 'avg_assist', 'avg_scoring', and 'avg_rebound'
    # fields is greater than 50
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "script": {
                            "script": {
                                "source": "doc['avg_assist'].value + doc['avg_scoring'].value + doc['avg_rebound'].value > 40"
                            }
                        }
                    }
                ]
            }
        }
    }
    # execute the search and print the results
    response = es.search(index=INDEX_NAME, body=query)

    # get only the documents
    for hit in response['hits']['hits']:
        print(hit['_source'])


if __name__ == '__main__':
    main()
