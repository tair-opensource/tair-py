#!/usr/bin/env python
from random import random

from conf_examples import get_tair

from tair import ResponseError

dim = 4
queries = [[random() for _ in range(dim)] for _ in range(2)]

# create an index
# @param index_name the name of index
# @param dims the dimension of vector
# @return success: True, fail: False.
def create_index(index_name: str):
    try:
        tair = get_tair()
        index_params = {
            "M": 32,
            "ef_construct": 200,
        }
        # index_params the params of index
        return tair.tvs_create_index(index_name, dim, **index_params)
    except ResponseError as e:
        print(e)
        return None

def get_index(index_name: str):
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_get_index(index_name)
    except ResponseError as e:
        print(e)
        return None

def scan_index():
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_scan_index()
    except ResponseError as e:
        print(e)
        return None

def hset(index_name:str):
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_hset(index_name,"key","[1.1,1.1,1.1,1.1]")
    except ResponseError as e:
        print(e)
        return None

def hget(index_name:str):
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_hgetall(index_name,"key")
    except ResponseError as e:
        print(e)
        return None

def scan(index_name:str):
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_scan(index_name)
    except ResponseError as e:
        print(e)
        return None

def knnsearch(index_name:str):
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_knnsearch(index_name,1,queries[0])
    except ResponseError as e:
        print(e)
        return None


def mknnsearch(index_name:str):
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_mknnsearch(index_name,1,queries)
    except ResponseError as e:
        print(e)
        return None

def mindexknnsearch():
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_mindexknnsearch(["test", "test2"],1,queries[0])
    except ResponseError as e:
        print(e)
        return None

def mindexmknnsearch():
    try:
        tair = get_tair()
        # index_params the params of index
        return tair.tvs_mindexmknnsearch(["test", "test2"],1,queries)
    except ResponseError as e:
        print(e)
        return None

# delete an index
# @param index_name the name of index
# @return success: True, fail: False.
def delete_index(index_name: str):
    try:
        tair = get_tair()
        return tair.tvs_del_index(index_name)
    except ResponseError as e:
        print(e)
        return False

if __name__ == "__main__":
    create_index("test")
    create_index("test2")
    get_index("test")
    scan_index()
    hset("test")
    hget("test")
    scan("test")
    knnsearch("test")
    mknnsearch("test")
    mindexknnsearch()
    mindexmknnsearch()
    delete_index("test")
