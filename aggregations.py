from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

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

    # Create a Search object and specify the index to search in
    s = Search(using=es, index=INDEX_NAME)

    # Add a query to match all documents in the index
    s = s.query("match_all")

    # Set the size parameter to 0 so that we don't get any search results back
    s = s.params(size=0)

    # Add an aggregation to group the data by team, and calculate the average score and average assists for each team
    s.aggs.bucket("by_team", "terms", field="team") \
        .metric("avg_score", "avg", field="avg_scoring") \
        .metric("avg_assist", "avg", field="avg_assist")

    # Execute the search
    response = s.execute()

    # Loop over the buckets of the by_team aggregation and print out the team name, average score, and average
    # assists for each team
    for team in response.aggregations.by_team.buckets:
        print(team.key, team.avg_score.value, team.avg_assist.value)


if __name__ == '__main__':
    main()
