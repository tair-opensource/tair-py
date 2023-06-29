# /user/bin/env python3
import os
import string
import sys
import unittest
import uuid
from random import choice, randint, random

import redis

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tair.tairvector import (
    Constants,
    DataType,
    DistanceMetric,
    TairVectorCommands,
    TairVectorIndex,
)

from .conftest import get_tair_client

client = get_tair_client()

dim = 16
num_vectors = 100
test_vectors = [[random() for _ in range(dim)] for _ in range(num_vectors)]
test2_vectors = [[random() for _ in range(dim)] for _ in range(num_vectors)]
num_attrs = 3
attr_keys = ["key-%d" % i for i in range(num_attrs)]
attr_values = [
    "".join(choice(string.ascii_uppercase + string.digits) for _ in range(4))
    for _ in range(num_vectors * num_attrs)
]
test_attributes = [
    dict(zip(attr_keys, attr_values[i : i + 3]))
    for i in range(0, num_vectors * num_attrs, num_attrs)
]

num_queries = 10
queries = [[random() for _ in range(dim)] for _ in range(num_queries)]


class IndexCommandsTest(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.index_params = {"M": 32, "ef_construct": 200}

    # the following test functions will execute in alphabetical order
    def test_0_delete_all_indices(self):
        # get all indices
        indices = []
        result = client.tvs_scan_index()
        for index in result.iter():
            indices.append(index)

        # deleting all indices
        for index in indices:
            try:
                ret = client.tvs_del_index(index)
                self.assertEqual(ret, 1)
            except:
                self.logger.error("delete index [%s] failed" % index)

        # scan indices again
        indices = []
        result = client.tvs_scan_index()
        for index in result.iter():
            indices.append(index)
        self.assertEqual(indices, [])

    def test_1_delete_noexist_index(self):
        ret = client.tvs_del_index("test")
        self.assertFalse(ret, 0)

    def test_2_get_nonexist_index(self):
        self.assertIsNone(client.tvs_get_index("test"))

    def test_3_create_index(self):
        ret = client.tvs_create_index("test", dim, **self.index_params)
        self.assertEqual(ret, True)

    def test_4_create_duplicate_index(self):
        with self.assertRaises(redis.exceptions.ResponseError):
            ret = client.tvs_create_index("test", dim, **self.index_params)

    def test_5_get_index(self):
        index = client.tvs_get_index("test")
        for k, v in self.index_params.items():
            self.assertTrue(k in index)
            self.assertEqual(index[k], str(v))

    def test_6_scan_index(self):
        indices = []
        result = client.tvs_scan_index()
        for index in result.iter():
            indices.append(index.decode())
        self.assertListEqual(indices, ["test"])

    def test_7_scan_index_with_pattern(self):
        indices = []
        result = client.tvs_scan_index(pattern="aaa")
        for index in result.iter():
            indices.append(index)
        self.assertEqual(indices, [])

    def test_8_delete_test_index(self):
        self.assertEqual(client.tvs_del_index("test"), 1)


def floatEqual(v1: float, v2: float, epsilon=1e-6) -> bool:
    delta = v1 - v2
    return delta >= -epsilon and delta <= epsilon


def vectorEqual(v1, v2) -> bool:
    if len(v1) != len(v2):
        return False
    for i in range(len(v1)):
        if not floatEqual(v1[i], v2[i]):
            return False
    return True


class DataCommandsTest(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.index_params = {
            "M": 32,
            "ef_construct": 200,
        }

    def test_0_init(self):
        # delete test index
        try:
            client.tvs_del_index("test")
        except:
            pass

    def test_1_hset_with_no_vector(self):
        self.assertTrue(client.tvs_create_index("test", dim, **self.index_params))
        for i, attrs in enumerate(test_attributes):
            ret = client.tvs_hset("test", str(i), vector=None, **attrs)
            self.assertEqual(ret, len(attrs))
        # get and check
        for i, attrs in enumerate(test_attributes):
            obj = client.tvs_hgetall("test", str(i))
            self.assertDictEqual(obj, attrs)
        self.assertEqual(client.tvs_del_index("test"), 1)

    def test_2_hset_with_no_attributes(self):
        self.assertTrue(client.tvs_create_index("test", dim, **self.index_params))
        for i, v in enumerate(test_vectors):
            ret = client.tvs_hset("test", str(i), vector=v)
            self.assertTrue(ret)
        # get and check
        for i, v in enumerate(test_vectors):
            obj = client.tvs_hgetall("test", str(i))
            self.assertTrue(Constants.VECTOR_KEY in obj)
            self.assertTrue(vectorEqual(v, obj[Constants.VECTOR_KEY]))

        self.assertEqual(client.tvs_del_index("test"), 1)

    def test_3_hset(self):
        self.assertTrue(client.tvs_create_index("test", dim, **self.index_params))
        for i, v in enumerate(test_vectors):
            ret = client.tvs_hset("test", str(i), vector=v, **test_attributes[i])
            self.assertTrue(ret)
        # get and check
        for i, v in enumerate(test_vectors):
            obj = client.tvs_hgetall("test", str(i))
            self.assertTrue(Constants.VECTOR_KEY in obj)
            self.assertTrue(vectorEqual(v, obj[Constants.VECTOR_KEY]))
            del obj[Constants.VECTOR_KEY]
            self.assertDictEqual(obj, test_attributes[i])

    def test_hmget(self):
        self.assertTrue(client.tvs_create_index("test", dim, **self.index_params))
        vector = [randint(1, 100) for _ in range(dim)]
        key = "key_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        ret = client.tvs_hset("test", key, vector=vector, field1=value1, field2=value2)
        self.assertTrue(ret)
        obj = client.tvs_hmget(
            "test", key, Constants.VECTOR_KEY, "field1", "field2", "field3"
        )
        self.assertEqual(len(obj[0].split(b",")), len(vector))
        self.assertEqual(obj[1], bytes(value1, encoding="ascii"))
        self.assertEqual(obj[2], bytes(value2, encoding="ascii"))

    def test_4_scan(self):
        result = client.tvs_scan("test")
        scanned_keys = []
        for k in result.iter():
            scanned_keys.append(k.decode())
        expected_keys = [str(i) for i in range(len(test_vectors))]
        self.assertSetEqual(set(scanned_keys), set(expected_keys))

    def test_5_scan_with_pattern(self):
        result = client.tvs_scan("test", pattern="aaa")
        scanned_keys = []
        for k in result.iter():
            scanned_keys.append(k)
        self.assertEqual(scanned_keys, [])

        result = client.tvs_scan("test", pattern="0")
        scanned_keys = []
        for k in result.iter():
            scanned_keys.append(k)
        self.assertEqual(scanned_keys, [b"0"])

    def test_6_hdel(self):
        for i, attr in enumerate(test_attributes):
            keys = attr.keys()
            self.assertEqual(client.tvs_hdel("test", str(i), *keys), len(keys))

    def test_7_delete(self):
        # delete inserted entries
        for i in range(len(test_vectors)):
            self.assertEqual(client.tvs_del("test", str(i)), 1)

    def test_9_delete(self):
        self.assertEqual(client.tvs_del_index("test"), 1)


class SearchCommandsTest(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.top_k = 10
        self.index_params = {
            "M": 32,
            "ef_construct": 200,
        }

    def test_0_init(self):
        # delete test index
        try:
            client.tvs_del_index("test")
            client.tvs_del_index("test2")
        except:
            pass

        ret = client.tvs_create_index("test", dim, **self.index_params)
        ret = client.tvs_create_index("test2", dim, **self.index_params)
        if not ret:
            raise RuntimeError("create test/test2 index failed")

    def test_1_insert_vectors(self):
        for i, v in enumerate(test_vectors):
            ret = client.tvs_hset("test", str(i).zfill(6), vector=v)
            self.assertTrue(ret)
        for i, v in enumerate(test2_vectors):
            ret = client.tvs_hset("test2", str(i).zfill(6), vector=v)
            self.assertTrue(ret)

    def test_2_knn_search(self):
        for q in queries:
            result = client.tvs_knnsearch("test", self.top_k, vector=q)
            self.assertEqual(self.top_k, len(result))
            d = 0.0
            for k, v in result:
                self.assertGreaterEqual(v, d)
                d = v

    def test_3_knn_search_with_params(self):
        for ef in range(self.top_k, 100, 10):
            for q in queries:
                result = client.tvs_knnsearch(
                    "test", self.top_k, vector=q, ef_search=ef
                )
                self.assertEqual(self.top_k, len(result))
                d = 0.0
                for k, v in result:
                    self.assertGreaterEqual(v, d)
                    d = v

    def test_4_knn_msearch(self):
        batch = queries[:2]
        result = client.tvs_mknnsearch("test", self.top_k, batch)
        self.assertEqual(len(result), len(batch))
        for r in result:
            d = 0.0
            for _, v in r:
                self.assertGreaterEqual(v, d)
                d = v

    def test_7_mindexknnsearch(self):
        indexs = ["test", "test2"]
        for q in queries:
            result = client.tvs_mindexknnsearch(indexs, self.top_k, vector=q)
            self.assertEqual(self.top_k, len(result))
            d = 0.0
            for k, v in result:
                self.assertGreaterEqual(v, d)
                d = v

    def test_8_mindexmknnsearch(self):
        indexs = ["test", "test2"]
        batch = queries[:2]
        result = client.tvs_mindexmknnsearch(indexs, self.top_k, batch)
        self.assertEqual(len(result), len(batch))
        for r in result:
            d = 0.0
            for _, v in r:
                self.assertGreaterEqual(v, d)
                d = v

    def test_9_delete_index(self):
        client.tvs_del_index("test")
        client.tvs_del_index("test2")


class IndexApiTest(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.index_params = {
            "M": 32,
            "ef_construct": 200,
        }

    def test_0_init(self):
        client.tvs_del_index("test")
        # try getting a non-existing index
        with self.assertRaises(ValueError):
            index = TairVectorIndex(client, "test")

    def test_1_create_index(self):
        # create a new index
        index = TairVectorIndex(client, "test", dim=dim, **self.index_params)
        self.assertIsNotNone(index)

        # get an existing index
        index2 = TairVectorIndex(client, "test")
        self.assertEqual(str(index), str(index2))

    def test_2_create_duplicate_index(self):
        with self.assertRaises(redis.exceptions.ResponseError):
            index = TairVectorIndex(client, "test", dim=dim, **self.index_params)

    def test_3_index_api(self):
        index = client.tvs_index("test")

        for i, v in enumerate(test_vectors):
            ret = index.tvs_hset(str(i), vector=v, **test_attributes[i])
            self.assertTrue(ret)
        # get and check
        for i, v in enumerate(test_vectors):
            obj = index.tvs_hgetall(str(i))
            self.assertTrue(Constants.VECTOR_KEY in obj)
            self.assertTrue(vectorEqual(v, obj[Constants.VECTOR_KEY]))
            del obj[Constants.VECTOR_KEY]
            self.assertDictEqual(obj, test_attributes[i])

        result = index.tvs_scan()
        scanned_keys = []
        for k in result.iter():
            scanned_keys.append(k.decode())
        expected_keys = [str(i) for i in range(len(test_vectors))]
        self.assertSetEqual(set(scanned_keys), set(expected_keys))

        for i, attr in enumerate(test_attributes):
            keys = attr.keys()
            self.assertEqual(index.tvs_hdel(str(i), *keys), len(keys))

        # delete inserted entries
        for i in range(len(test_vectors)):
            self.assertEqual(index.tvs_del(str(i)), 1)


dim_bin_vector = 16
num_bin_vectors = 1000
test_bin_vectors = [
    [randint(0, 1) for _ in range(dim_bin_vector)] for _ in range(num_bin_vectors)
]


def jaccard(x, y):
    intersect = 0
    union = 0
    for i in range(len(x)):
        if x[i] == 1 or y[i] == 1:
            union += 1
        if x[i] == 1 and y[i] == 1:
            intersect += 1
    return 1 - intersect / union


class BinaryIndexTest(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.index_params = {
            "M": 32,
            "ef_construct": 200,
        }

    def test_0_create(self):
        ret = client.tvs_create_index(
            "test_bin",
            dim_bin_vector,
            distance_type=DistanceMetric.Jaccard,
            data_type=DataType.Binary,
            **self.index_params
        )
        self.assertTrue(ret)

        # try create index with invalid parameters
        with self.assertRaises(redis.exceptions.ResponseError):
            ret = client.tvs_create_index(
                "test_bin2",
                dim_bin_vector,
                distance_type=DistanceMetric.L2,
                data_type=DataType.Binary,
                **self.index_params
            )

    def test_1_hset_hget(self):
        for i, v in enumerate(test_bin_vectors):
            ret = client.tvs_hset("test_bin", str(i), v, True)
            self.assertEqual(ret, 1)
        for i, v in enumerate(test_bin_vectors):
            ret = client.tvs_hgetall("test_bin", str(i))
            self.assertEqual(v, list(ret[Constants.VECTOR_KEY]))

    def test_2_knnsearch(self):
        q = [randint(0, 1) for _ in range(dim_bin_vector)]
        ret = client.tvs_knnsearch("test_bin", 10, q, True)
        last_dist = 0.0

        for k, s in ret:
            self.assertGreaterEqual(s, last_dist)
            last_dist = s
            self.assertAlmostEqual(s, jaccard(q, test_bin_vectors[int(k)]))

    def test_3_index_api(self):
        ret = client.tvs_del_index("test_bin")
        self.assertEqual(ret, 1)
        with self.assertRaises(ValueError):
            index = TairVectorIndex(client, "test_bin")

        index = TairVectorIndex(
            client,
            "test_bin",
            dim=dim_bin_vector,
            distance_type=DistanceMetric.Jaccard,
            data_type=DataType.Binary,
            **self.index_params
        )
        self.assertIsNotNone(index)

        index2 = TairVectorIndex(client, "test_bin")
        self.assertEqual(str(index), str(index2))

        for i, v in enumerate(test_bin_vectors):
            ret = index.tvs_hset(str(i), v)
            self.assertEqual(ret, 1)

        for i, v in enumerate(test_bin_vectors):
            ret = index.tvs_hgetall(str(i))
            self.assertEqual(v, list(ret[Constants.VECTOR_KEY]))

        q = [randint(0, 1) for _ in range(dim_bin_vector)]
        ret = index.tvs_knnsearch(10, q)
        last_dist = 0.0

        for k, s in ret:
            self.assertGreaterEqual(s, last_dist)
            last_dist = s
            self.assertAlmostEqual(s, jaccard(q, test_bin_vectors[int(k)]))

    def test_9_cleanup(self):
        ret = client.tvs_del_index("test_bin")
        self.assertEqual(ret, 1)


filter_test_data = [
    {"vector": "[7,3]", "name": "Aaron", "age": "12"},  # dist 58
    {"vector": "[9,2]", "name": "Bob", "age": "33"},  # dist 85
    {"vector": "[6,6]", "name": "Charlie", "age": "29"},  # dist 72
    {"vector": "[3,5]", "name": "Daniel", "age": "23"},  # dist 34
    {"vector": "[3,7]", "name": "Eason", "age": "22"},  # dist 58
    {"vector": "[3,6]", "name": "Fabian", "age": "35"},  # dist 45
    {"vector": "[5,2]", "name": "George", "age": "12"},  # dist 29
    {"vector": "[8,9]", "name": "Henry", "age": "30"},  # dist 145
    {"vector": "[5,5]", "name": "Ivan", "age": "16"},  # dist 50
    {"vector": "[2,7]", "name": "James", "age": "12"},  # dist 53
]


class ScanTest(unittest.TestCase):
    index_name = "scan_test"

    def test_0_create(self):
        ret = client.tvs_create_index(
            self.index_name,
            2,
            distance_type=DistanceMetric.L2,
        )
        self.assertTrue(ret)
        for i, d in enumerate(filter_test_data):
            ret = client.tvs_hset(self.index_name, str(i), **d)
            self.assertEqual(ret, 3)

    def test_1_basic_scan(self):
        result = [r for r in client.tvs_scan(self.index_name, "*")]
        result = sorted(result)
        self.assertListEqual(result, [str(i).encode() for i in range(0, 10)])

        result = [r for r in client.tvs_scan(self.index_name, "a*")]
        self.assertEqual(len(result), 0)

    def test_2_scan_with_filter(self):
        result = [r for r in client.tvs_scan(self.index_name, filter_str="age>30")]
        result = sorted(result)
        self.assertListEqual(result, [b"1", b"5"])

        result = [r for r in client.tvs_scan(self.index_name, filter_str='name>"H"')]
        result = sorted(result)
        self.assertListEqual(result, [b"7", b"8", b"9"])

        result = [
            r for r in client.tvs_scan(self.index_name, filter_str='name=="Henry"')
        ]
        self.assertListEqual(result, [b"7"])

    def test_3_scan_with_max_distance(self):
        with self.assertRaises(ValueError):
            client.tvs_scan(self.index_name, max_dist=50)
        with self.assertRaises(ValueError):
            client.tvs_scan(self.index_name, vector=[0, 0])
        result = [
            r for r in client.tvs_scan(self.index_name, vector=[0, 0], max_dist=50)
        ]
        result = sorted(result)
        self.assertListEqual(result, [b"3", b"5", b"6"])

    def test_4_scan_with_both(self):
        result = [
            r
            for r in client.tvs_scan(
                self.index_name, vector=[0, 0], max_dist=50, filter_str="age<30"
            )
        ]
        result = sorted(result)
        self.assertListEqual(result, [b"3", b"6"])

    def test_9_cleanup(self):
        ret = client.tvs_del_index(self.index_name)
        self.assertEqual(ret, 1)


class SearchWithFilterTest(unittest.TestCase):
    index_name = "filter_test"

    def test_0_create(self):
        ret = client.tvs_create_index(
            self.index_name,
            2,
            distance_type=DistanceMetric.L2,
        )
        self.assertTrue(ret)
        for i, d in enumerate(filter_test_data):
            ret = client.tvs_hset(self.index_name, str(i), **d)
            self.assertEqual(ret, 3)

    def test_1_knnsearch(self):
        result = client.tvs_knnsearch(self.index_name, 5, vector=[0, 0])
        self.assertListEqual([t[0] for t in result], [b"6", b"3", b"5", b"8", b"9"])
        result = client.tvs_knnsearch(self.index_name, 5, vector=[0, 0], MAX_DIST=50)
        self.assertListEqual([t[0] for t in result], [b"6", b"3", b"5", b"8"])
        result = client.tvs_knnsearch(
            self.index_name, 5, vector=[0, 0], filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result], [b"6", b"8", b"9", b"0"])
        result = client.tvs_knnsearch(
            self.index_name, 5, vector=[0, 0], MAX_DIST=50, filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result], [b"6", b"8"])

    def test_2_mknnsearch(self):
        result = client.tvs_mknnsearch(self.index_name, 5, [[0, 0]])
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"3", b"5", b"8", b"9"])
        result = client.tvs_mknnsearch(self.index_name, 5, [[0, 0]], MAX_DIST=50)
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"3", b"5", b"8"])
        result = client.tvs_mknnsearch(
            self.index_name, 5, [[0, 0]], filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"8", b"9", b"0"])
        result = client.tvs_mknnsearch(
            self.index_name, 5, [[0, 0]], MAX_DIST=50, filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"8"])

    def test_3_mindexknnsearch(self):
        result = client.tvs_mindexknnsearch([self.index_name], 5, vector=[0, 0])
        self.assertListEqual([t[0] for t in result], [b"6", b"3", b"5", b"8", b"9"])
        result = client.tvs_mindexknnsearch(
            [self.index_name], 5, vector=[0, 0], MAX_DIST=50
        )
        self.assertListEqual([t[0] for t in result], [b"6", b"3", b"5", b"8"])
        result = client.tvs_mindexknnsearch(
            [self.index_name], 5, vector=[0, 0], filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result], [b"6", b"8", b"9", b"0"])
        result = client.tvs_mindexknnsearch(
            [self.index_name], 5, vector=[0, 0], MAX_DIST=50, filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result], [b"6", b"8"])

    def test_4_mindexmknnsearch(self):
        result = client.tvs_mindexmknnsearch([self.index_name], 5, [[0, 0]])
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"3", b"5", b"8", b"9"])
        result = client.tvs_mindexmknnsearch(
            [self.index_name], 5, [[0, 0]], MAX_DIST=50
        )
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"3", b"5", b"8"])
        result = client.tvs_mindexmknnsearch(
            [self.index_name], 5, [[0, 0]], filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"8", b"9", b"0"])
        result = client.tvs_mindexmknnsearch(
            [self.index_name], 5, [[0, 0]], MAX_DIST=50, filter_str="age<20"
        )
        self.assertListEqual([t[0] for t in result[0]], [b"6", b"8"])

    def test_9_cleanup(self):
        ret = client.tvs_del_index(self.index_name)
        self.assertEqual(ret, 1)


if __name__ == "__main__":
    unittest.main()
    client.close()
