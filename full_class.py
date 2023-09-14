import logging
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, NotFoundError, ConflictError, RequestError
from logger import Logger


class ElasticsearchClient:
    def __init__(self, path, username, password):
        self.path = path
        self.username = username
        self.password = password
        self.es = None
        self.logger = Logger(__name__)
        self.connect_to_elastic(path=path, username=username, password=password)

    def connect_to_elastic(self, path, username, password):
        try:
            self.es = Elasticsearch(
                path,
                verify_certs=False,
                basic_auth=(username, password))
            if self.es.ping():
                self.logger.info("Connected to Elasticsearch cluster")
        except ConnectionError as e:
            self.logger.error(f"Connection failed: {e}")

    def create_index(self, index_name, body):
        try:
            response = self.es.indices.create(index=index_name, body=body)
            self.logger.info(response)
        except ConflictError as e:
            self.logger.warning(f"Index already exists: {e}")
        except RequestError as e:
            self.logger.error(f"Invalid index name or mapping: {e}")

    def index_document(self, index_name, doc_id, body):
        try:
            response = self.es.index(index=index_name, id=doc_id, body=body)
            self.logger.info(response)
        except NotFoundError as e:
            self.logger.warning(f"Index or document not found: {e}")
        except RequestError as e:
            self.logger.error(f"Invalid request: {e}")

    def get_document(self, index_name, doc_id):
        try:
            response = self.es.get(index=index_name, id=doc_id)
            self.logger.info(response['_source'])
        except NotFoundError as e:
            self.logger.warning(f"Index or document not found: {e}")
        except RequestError as e:
            self.logger.error(f"Invalid request: {e}")

    def update_document_by_id(self, index_name, doc_id, body):
        try:
            response = self.es.update(index=index_name, id=doc_id, body=body)
            self.logger.info(response)
        except NotFoundError as e:
            self.logger.warning("Index or document not found: {e}")
        except RequestError as e:
            self.logger.error("Invalid request: {e}")

    def delete_document(self, index_name, doc_id):
        try:
            response = self.es.delete(index=index_name, id=doc_id)
            self.logger.info(response)
        except NotFoundError as e:
            self.logger.warning(f"Index or document not found: {e}")
        except RequestError as e:
            self.logger.error(f"Invalid request: {e}")

    def bulk_index_documents(self, index_name, documents):
        try:
            actions = [
                {
                    '_index': index_name,
                    '_id': doc['id'],
                    '_source': doc
                }
                for doc in documents
            ]
            response = helpers.bulk(self.es, actions)
            self.logger.info(response)
        except NotFoundError as e:
            self.logger.warning(f"Index or document not found: {e}")
        except RequestError as e:
            self.logger.error(f"Invalid request: {e}")

    def scan_index(self, index_name):
        try:
            response = helpers.scan(self.es, index=index_name)
            for doc in response:
                yield doc['_source']
        except NotFoundError as e:
            self.logger.warning(f"Index or document not found: {e}")
        except RequestError as e:
            self.logger.error(f"Invalid request: {e}")


if __name__ == '__main__':
    ELASTIC_PASSWORD = "6fX0s0E23oIeF4BGkYY9"
    ELASTIC_USERNAME = "elastic"
    ELASTIC_PATH = "http://localhost:9200"
    INDEX_NAME = "nba_players"
    elastic_instance = ElasticsearchClient(path=ELASTIC_PATH,
                                           username=ELASTIC_USERNAME,
                                           password=ELASTIC_PASSWORD)
    elastic_instance.delete_document(index_name=INDEX_NAME, doc_id=2)
    