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
        return tair.tft_createindex(index, schema)
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
    except ResponseError as e:
        print(e)
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
            "title": {
                "type": "keyword"
            },
            "content": {
                "type": "text",
                "analyzer": "jieba"
            },
            "time": {
                "type": "long"
            },
            "author": {
                "type": "keyword"
            },
            "heat": {
                "type": "integer"
            }
        }
    }
}"""

json2 = """{
    "title": "Does not work",
    "content": "It was removed from the beta a while ago. You should have expected it was going to be removed from the stable client as well at some point.",
    "time": 1541713787,
    "author": "cSg|mc",
    "heat": 10
}"""

json3 = """{
    "title": "paypal no longer launches to purchase",
    "content": "Since the last update, I cannot purchase anything via the app. I just keep getting a screen that says",
    "time": 1551476987,
    "author": "disasterpeac",
    "heat": 2
}"""

json4 = """{
    "title": "cat not login",
    "content": "Hey! I am trying to login to steam beta client via qr code / steam guard code but both methods does not work for me",
    "time": 1664488187,
    "author": "7xx",
    "heat": 100
}"""

json5 = """{
    "sort": [
        {
            "heat": {
                "order": "desc"
            }
        }
    ],
    "query": {
        "match": {
            "content": "paypal work code"
        }
    }
}"""


if __name__ == "__main__":
    key = "FullTextSearch"
    create_index(key, json1)
    add_doc(key, json2)
    add_doc(key, json3)
    add_doc(key, json4)
    # search index
    print(search_index(key, json5))
