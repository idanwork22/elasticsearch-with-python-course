from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

# Elasticsearch credentials and endpoint
ELASTIC_PASSWORD = "6fX0s0E23oIeF4BGkYY9"
ELASTIC_USERNAME = "elastic"
ELASTIC_PATH = "http://localhost:9200"

# Index name for NBA player documents
INDEX_NAME = "nba_players"


def connect_to_elastic() -> Elasticsearch:
    """Connect to Elasticsearch and return the client object"""
    client = Elasticsearch(
        ELASTIC_PATH,
        verify_certs=False,
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
    )
    return client


def main():
    # Connect to Elasticsearch
    es = connect_to_elastic()

    # Define a query to match documents where the 'date_of_birth' field is between 1980-01-01 and 1990-12-31
    s = Search(using=es, index=INDEX_NAME) \
        .query(Q("range", date_of_birth={"gte": "1980-01-01", "lte": "1990-12-31"}))

    # Execute the search and get the response object
    response = s.execute()

    # Print the full response from Elasticsearch
    print(f"response: \n {response}")

    # Iterate over the search results and print the first name, last name, and date of birth for each hit
    for hit in response:
        print(hit.first_name, hit.last_name, hit.date_of_birth)


if __name__ == '__main__':
    main()
