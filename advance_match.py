from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

# define constants
ELASTIC_PASSWORD = "6fX0s0E23oIeF4BGkYY9"
ELASTIC_USERNAME = "elastic"
ELASTIC_PATH = "http://localhost:9200"
INDEX_NAME = "nba_players"

# connect to Elasticsearch
def connect_to_elastic() -> Elasticsearch:
    client = Elasticsearch(
        ELASTIC_PATH,
        verify_certs=False,
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
    )
    return client

# main function
def main():
    # connect to Elasticsearch
    es = connect_to_elastic()

    # create a search object with a range and terms query
    s = Search(using=es, index=INDEX_NAME) \
        .query(Q("bool",
                 must=[Q("range", avg_scoring={"gte": 20, "lte": 25}),
                       Q("terms", country=["Greece", "USA"])]))

    # execute the search and print the results
    response = s.execute()

    # see full response from the server
    print(f"response: \n {response}")

    # loop over the hits and print the fields we are interested in
    for hit in response:
        print(hit.first_name, hit.last_name, hit.avg_scoring, hit.country)


if __name__ == '__main__':
    main()
