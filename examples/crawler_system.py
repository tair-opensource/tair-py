#!/usr/bin/env python

from typing import List

from conf_examples import get_tair

from tair import ResponseError


# Determine if the URL has been crawled
# @param key key
# @param urls the urls
def bf_mexists(key: str, urls: List[str]):
    try:
        tair = get_tair()
        return tair.bf_mexists(key, urls)
    except ResponseError as e:
        print(e)
        return None


if __name__ == "__main__":
    tair = get_tair()
    key = "CrawlerSystem"
    tair.bf_add(key, "abc")
    tair.bf_add(key, "def")
    tair.bf_add(key, "ghi")
    print(bf_mexists(key, ["abc", "def", "xxx"]))
