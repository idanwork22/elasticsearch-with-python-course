from elasticsearch import Elasticsearch

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
    document_to_update = {
        "doc": {'first_name': 'LeBron', 'last_name': 'James', 'date_of_birth': '1984-12-30', 'position': 'PF',
                'team': 'BEST UDEMY COURSE',
                'avg_scoring': 25.4, 'avg_rebound': 7.9, 'avg_assist': 7.9, 'country': 'USA'}}
    client = connect_to_elastic()
    result = client.update(index=INDEX_NAME, id=0, body=document_to_update)
    print(result)


if __name__ == '__main__':
    main()
