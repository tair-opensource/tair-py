#!/usr/bin/env python
from random import random
import time
from conf_examples import get_tair
from tair import ResponseError
from tair.tairvector import DistanceMetric, IndexType

dim = 4
queries = [[random() for _ in range(dim)] for _ in range(2)]
total_count = 20
vector_ids = [str(i) for i in range(1, total_count + 1)]

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

def ttl(index_name:str):
    try:
        tair = get_tair()
        tair.tvs_hset(index_name, "key_ttl", vector=[random() for _ in range(dim)])
        tair.tvs_hset(index_name, "key_ttl2", vector=[random() for _ in range(dim)])
        tair.tvs_hset(index_name, "key_ttl3", vector=[random() for _ in range(dim)])
        tair.tvs_hset(index_name, "key_ttl4", vector=[random() for _ in range(dim)])
        # set relative expiration time: s
        tair.tvs_hexpire(index_name, "key_ttl", 5)
        tair.tvs_httl(index_name, "key_ttl")
        # set relative expiration time: ms
        tair.tvs_hpexpire(index_name, "key_ttl2", 5000)
        tair.tvs_hpttl(index_name, "key_ttl2")
        # set absolute expiration time: s
        abs_expire = int(time.time()) + 5
        tair.tvs_hexpireat(index_name, "key_ttl3", abs_expire)
        tair.tvs_hexpiretime(index_name, "key_ttl3")
        tair.tvs_httl(index_name, "key_ttl3")
        # set absolute expiration time: ms
        abs_expire = int(time.time() * 1000) + 100
        tair.tvs_hpexpireat(index_name, "key_ttl4", abs_expire)
        tair.tvs_hpexpiretime(index_name, "key_ttl4")    
    except ResponseError as e:
        print(e)
        return None
    
def hybrid_search(index_name:str):
    try:
        tair = get_tair()
        kwargs = {"lexical_algorithm":"bm25"}
        # create hybrid_search index
        tair.tvs_create_index(index_name, dim=dim, distance_type=DistanceMetric.L2, index_type=IndexType.HNSW, **kwargs)
        # init data
        vectors = [[i, i, i, i] for i in range(1, 4)]
        texts = [{"TEXT": "Turtle Check Men Navy Blue Shirt"}, {"TEXT":"Peter England Men Party Blue Jeans"}, {"TEXT": "Titan Women Silver Watch"}]
        for i in range(3):
            tair.tvs_hset(index=index_name, key=str.format("key{}",i), vector=vectors[i], is_binary=False, **texts[i])
        search_text = "Women Watch"
        search_vector = "[1,1,1,0]"
        # hybrid_ratio 0: only text search; 0.5: vector and text hybrid search; 1: only vector search
        hybrid_ratios = [0, 0.5, 1]
        for hybrid_ratio in hybrid_ratios:
            kwargs = {"TEXT": search_text, "hybrid_ratio": hybrid_ratio}   
            # only text search:              [(b'key2', 0.9843749403953552), (b'key0', 61.000003814697266), (b'key1', 62.0)]
            # vector and text hybrid search: [(b'key2', 30.991933822631836), (b'key0', 61.000003814697266), (b'key1', 62.0)]
            # only vector search:            [(b'key0', 61.000003814697266), (b'key1', 62.0), (b'key2', 62.99993133544922)]
            tair.tvs_knnsearch(index=index_name, k=10, vector=search_vector, is_binary=False, filter_str=None, **kwargs)
    except ResponseError as e:
        print(e)
        return None
    
# tvs.hset using pipeline
def pipeline_hset(index_name:str):
    tair = get_tair()
    pipeline = tair.pipeline(transaction=False)
    for i in range(total_count):
        vec_random = [random() for _ in range(dim)]
        attrs = {"flag" : i+1}
        pipeline.tvs_hset(index=index_name, key=vector_ids[i], vector=vec_random, is_binary=False, **attrs)
    results = pipeline.execute(raise_on_error=False)
    for result in results:
        try:
            # the return format of tvs.hset is the same as hset. It returns the number of successfully written attributes. 
            # if an attribute is overwritten, 0 is returned.
            # you can refer to tair/commands.py to query the return result format of each command.
            print(int(result))
        except Exception as e:
            print(result)
    
# tvs.hgetall using pipeline
def pipeline_hgetall(index_name:str):
    tair = get_tair()
    pipeline = tair.pipeline(transaction=False)
    for id in vector_ids:
        pipeline.tvs_hgetall(index=index_name,  key=id)
    results = pipeline.execute(raise_on_error=False)
    for result in results:
        # you can refer to tair/commands.py to query the return result format of each command.
        try:
            for key,value in result.items():
                print(f"{key}: {value}", end="\t")
            print("")
        except Exception as e:
            print(e) 

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
    ttl("test")
    hybrid_search("hybrid_search_test")
    pipeline_hset("test")
    pipeline_hgetall("test")
    delete_index("test")
    delete_index("test2")
    delete_index("hybrid_search_test")
    