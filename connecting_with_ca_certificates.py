from elasticsearch import Elasticsearch


def main():
    # Password for the 'elastic' user generated by Elasticsearch
    ELASTIC_PASSWORD = "6fX0s0E23oIeF4BGkYY9"
    # Create the client instance
    client = Elasticsearch(
        "http://localhost:9200",
        verify_certs=False,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )

    # Successful response!
    print(client.info())


if __name__ == '__main__':
    main()
