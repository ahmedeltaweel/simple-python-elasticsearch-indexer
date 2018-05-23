#! ./venv/bin/python3
import json
import os
from elasticsearch import Elasticsearch


DIR_PATH = os.path.dirname(__file__)
APS_FILE_PATH = os.path.join(DIR_PATH, 'products.json')
INDEX_NAME = 'products'
DOC_TYPE = 'product'
PATCH_SIZE = 1000

es = Elasticsearch(host="localhost", port=9200)


def load_json(filename):
    " Use a generator, no need to load all in memory"
    with open(filename, 'r') as open_file:
        for i in json.load(open_file):
            yield i


def create_index():
    if es.indices.exists(INDEX_NAME):
        print("deleting {} index...".format(INDEX_NAME))
        res = es.indices.delete(index=INDEX_NAME)
        print(" response: {}".format(res))

    request_body = {
        "product": {
            "properties": {
                "suggest": {
                    "type": "completion"
                }
            }
        }
    }
    print("creating {} index...".format(INDEX_NAME))
    res = es.indices.create(index=INDEX_NAME)
    print(" response: {}".format(res))
    print("adding mapping...")
    res = es.indices.put_mapping(index=INDEX_NAME, doc_type=DOC_TYPE, body=request_body)
    print(" response: {}".format(res))


def main():
    create_index()

    x = load_json(APS_FILE_PATH)
    count = 1
    for i in x:
        i.update({"suggest": {"input": i['name']}})
        print(i)
        print("document updated")
        es.create(index=INDEX_NAME, doc_type=DOC_TYPE, id=i['sku'], body=i)
        print('document with id {} indexed'.format(i['sku']))
        count += 1
        if count == PATCH_SIZE:
            break


if __name__ == '__main__':
    main()
