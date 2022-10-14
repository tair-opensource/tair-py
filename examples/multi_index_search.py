#!/usr/bin/env python

from tair import ResponseError

from conf_examples import get_tair


# create index, The field of index is parsed according to the field corresponding to the text
# @param index the index
# @param schema the index schema
# @return success: true, fail: false.
def create_index(index: str, schema: str) -> bool:
    try:
        tair = get_tair()
        tair.tft_createindex(index, schema)
        return True
    except ResponseError as e:
        print(e)

        return False


# Add doc to index, doc is JSON format.
# @param index the index
# @param doc the doc content
# @return unique doc id
def add_doc(index: str, doc: str):
    try:
        tair = get_tair()
        return tair.tft_adddoc(index, doc)
    except:
        return None


# search index by request
# @param index the index
# @param request the request
# @return
def search_index(index: str, request: str):
    try:
        tair = get_tair()
        return tair.tft_search(index, request)
    except ResponseError as e:
        print(e)
        return None


json1 = """{
    "mappings": {
        "properties": {
            "departure": {
                "type": "keyword"
            },
            "destination": {
                "type": "keyword"
            },
            "date": {
                "type": "keyword"
            },
            "seat": {
                "type": "keyword"
            },
            "with": {
                "type": "keyword"
            },
            "flight_id": {
                "type": "keyword"
            },
            "price": {
                "type": "double"
            },
            "departure_time": {
                "type": "long"
            },
            "destination_time": {
                "type": "long"
            }
        }
    }
}"""

json2 = """{
    "departure": "zhuhai",
    "destination": "hangzhou",
    "date": "2022-09-01",
    "seat": "first",
    "with": "baby",
    "flight_id": "CZ1000",
    "price": 986.1,
    "departure_time": 1661991010,
    "destination_time": 1661998210
}"""

json3 = """{
    "sort": [
        "departure_time"
    ],
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "date": "2022-09-01"
                    }
                },
                {
                    "term": {
                        "seat": "first"
                    }
                }
            ]
        }
    }
}"""

if __name__ == "__main__":
    key = "MultiIndexSearch"
    # create index
    create_index(key, json1)
    # add doc
    add_doc(key, json2)
    # search index
    search_index(key, json3)
