#!/usr/bin/env python

from conf_examples import get_tair

if __name__ == "__main__":
    tair = get_tair()

    key = "JSONDocument"
    json = """{
    "name": "tom",
    "age": 22,
    "description": "A man with a blue lightsaber",
    "friends": []
}"""
    tair.json_set(key, ".", json)
    description = tair.json_get(key, ".description").decode()
    print(description)
