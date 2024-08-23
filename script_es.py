from elasticsearch import Elasticsearch, NotFoundError, ConnectionError

class ElasticsearchClient:
    def __init__(self, host='localhost', port=9200, scheme='http'):
        self.host = host
        self.port = port
        self.scheme = scheme
        self.es = None

    def connect(self):
        try:
            self.es = Elasticsearch([{'host': self.host, 'port': self.port, 'scheme': self.scheme}])
            if self.es.ping():
                print("Conectado ao Elasticsearch!")
            else:
                print("Falha na conexão.")
        except ConnectionError as e:
            print(f"Erro de conexão: {e}")

    def create_index(self, index_name):
        try:
            self.es.indices.create(index=index_name, ignore=400)
            print(f"Índice '{index_name}' criado ou já existe.")
        except Exception as e:
            print(f"Erro ao criar índice: {e}")

    def index_document(self, index_name, doc_id, document):
        try:
            res = self.es.index(index=index_name, id=doc_id, document=document)
            print(f"Documento indexado: {res['result']}")
        except Exception as e:
            print(f"Erro ao indexar documento: {e}")

    def get_document(self, index_name, doc_id):
        try:
            res = self.es.get(index=index_name, id=doc_id)
            print(f"Documento recuperado: {res['_source']}")
        except NotFoundError:
            print("Documento não encontrado.")
        except Exception as e:
            print(f"Erro ao recuperar documento: {e}")

    def close(self):
        if self.es:
            self.es.transport.close()
            print("Conexão fechada.")

if __name__ == "__main__":
    client = ElasticsearchClient()
    client.connect()
    client.create_index('meu_indice')
    
    doc = {'name': 'John Doe', 'age': 29, 'occupation': 'developer'}
    client.index_document('meu_indice', 1, doc)
    
    client.get_document('meu_indice', 1)
    
    client.close()
