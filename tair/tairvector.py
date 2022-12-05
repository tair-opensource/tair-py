from typing import Sequence, Tuple, Union,Iterable
from tair.typing import ResponseT
from typing import Dict, List, Tuple, Union
from redis.client import pairs_to_dict
from redis.utils import str_if_bytes
from typing import Sequence, Union
from functools import partial, reduce

VectorType = Sequence[Union[int, float]]

class DistanceMetric:
    Euclidean = "L2"  # an alias to L2
    L2 = "L2"
    InnerProduct = "IP"
    Jaccard = "JACCARD"

class IndexType:
    HNSW = "HNSW"
    FLAT = "FLAT"

class Constants:
    VECTOR_KEY = "VECTOR"

class DataType:
    Float32 = "FLOAT32"
    Binary = "BINARY"

class TextVectorEncoder:
    SEP = bytes(",", "ascii")
    BITS = ("0", "1")

    def encode(vector: Sequence[Union[float, int]], is_binary=False) -> bytes:
        s = ""
        if is_binary:
            s = "[" + ",".join([TextVectorEncoder.BITS[x] for x in vector]) + "]"
        else:
            s = "[" + ",".join(["%f" % x for x in vector]) + "]"
        return bytes(s, encoding="ascii")  # ascii is enough

    def decode(buf: bytes) -> Tuple[float]:
        if buf[0] != ord("[") or buf[-1] != ord("]"):
            raise ValueError("invalid text vector value")
        is_int = True
        components = buf[1:-1].split(TextVectorEncoder.SEP)
        for x in components:
            if not x.isdigit():
                is_int = False

        if is_int:
            return tuple(int(x) for x in components)
        return tuple(float(x) for x in components)

class TairVectorScanResult:
    """
    wrapper for the results of scan commands
    """

    def __init__(self, client, get_batch_func):
        self.client = client
        self.get_batch = get_batch_func

    def __iter__(self):
        self.cursor = "0"
        self.batch = []
        self.idx = 0
        return self

    def __next__(self):
        if self.idx >= len(self.batch):
            if self.cursor is None:
                # iteration finished
                raise StopIteration

            # fetching next batch from server
            res = self.get_batch(self.cursor)
            # server returns cursor "0" means no more data to scan
            if res[0] == b"0":
                self.cursor = None
            else:
                self.cursor = res[0]
            self.batch = res[1]
            self.idx = 0
        if self.idx >= len(self.batch):
            # in case the first batch from server is empty
            raise StopIteration
        ret = self.batch[self.idx]
        self.idx += 1
        return ret.decode("utf-8")

    def iter(self):
        """
        create an iterator from the result
        """
        return iter(self)


class TairVectorIndex:
    def __init__(self, client, name, **index_params):
        self.client = client
        self.name = name

        # create new index
        if len(index_params) > 0:
            self.client.tvs_create_index(name, **index_params)

        self.params = self.client.tvs_get_index(name)
        if self.params is None:
            # not exist
            raise ValueError("index not exist")

        self.is_binary = False
        if self.params.get("data_type", None) == DataType.Binary:
            self.is_binary = True

        # bind methods
        for method in (
            "tvs_del",
            "tvs_hdel",
            "tvs_hgetall",
            "tvs_hmget",
            "tvs_scan",
        ):
            attr = getattr(TairVectorCommands, method)
            if callable(attr):
                setattr(self, method, partial(attr, self.client, self.name))

    def get(self):
        """get and update index info"""
        self.params = self.client.tvs_get_index(self.name)
        if self.params is None:
            # not exist
            raise ValueError("index not exist")
        return self.params

    def tvs_hset(self, key: str, vector: Union[VectorType, str] = None, **kwargs):
        """add/update a data entry to index
        @key: key for the data entry
        @vector: optional, vector value of the data entry
        @kwargs: optional, attribute pairs for the data entry
        """
        return self.client.tvs_hset(self.name, key, vector, self.is_binary, **kwargs)

    def tvs_knnsearch(
        self, k: int, vector: Union[VectorType, str], filter_str: str = None, **kwargs
    ):
        """search for the top @k approximate nearest neighbors of @vector"""
        return self.client.tvs_knnsearch(
            self.name, k, vector, self.is_binary, filter_str, **kwargs
        )

    def tvs_mknnsearch(
        self, k: int, vectors: Sequence[VectorType], filter_str: str = None, **kwargs
    ):
        """batch approximate nearest neighbors search for a list of vectors"""
        return self.client.tvs_knnsearch(
            self.name, k, vectors, self.is_binary, filter_str, **kwargs
        )

    def __str__(self):
        return "%s[%s]" % (self.name, self.params)

    def __repr__(self):
        return str(self)


class TairVectorCommands:

    encode_vector = TextVectorEncoder.encode
    decode_vector = TextVectorEncoder.decode

    CREATE_INDEX_CMD = "TVS.CREATEINDEX"
    GET_INDEX_CMD = "TVS.GETINDEX"
    DEL_INDEX_CMD = "TVS.DELINDEX"
    SCAN_INDEX_CMD = "TVS.SCANINDEX"

    def tvs_create_index(
        self,
        name: str,
        dim: int,
        distance_type: str = DistanceMetric.L2,
        index_type: str = IndexType.HNSW,
        data_type: str = DataType.Float32,
        **kwargs
    ):
        """
        create a vector
          @distance_type: distance metric type (L2/IP).
          @index_type: type of the index (HNSW/FLAT).
        keyword arguments
          @ef_construct: efConstruct for HNSW index (available if index_type == HNSW).
          @M: M for HNSW index (available if index_type == HNSW).
        """
        params = reduce(lambda x, y: x + y, kwargs.items(), ())
        return self.execute_command(
            self.CREATE_INDEX_CMD,
            name,
            dim,
            index_type,
            distance_type,
            "data_type",
            data_type,
            *params
        )

    def tvs_get_index(self, name: str):
        """
        get the infomation of an index
        """
        return self.execute_command(self.GET_INDEX_CMD, name)

    def tvs_del_index(self, name: str):
        """
        delete an index and all its data
        """
        return self.execute_command(self.DEL_INDEX_CMD, name)

    def tvs_scan_index(
        self, pattern: str = None, batch: int = 10
    ) -> TairVectorScanResult:
        """
        scan all the indices
        """
        args = ([] if pattern is None else ["MATCH", pattern]) + ["COUNT", batch]
        get_batch = lambda c: self.execute_command(self.SCAN_INDEX_CMD, c, *args)

        return TairVectorScanResult(self, get_batch)

    def tvs_index(self, name: str, **index_params) -> TairVectorIndex:
        """
        get or create an index
        """
        return TairVectorIndex(self, name, **index_params)

    HSET_CMD = "TVS.HSET"
    DEL_CMD = "TVS.DEL"
    HDEL_CMD = "TVS.HDEL"
    HGETALL_CMD = "TVS.HGETALL"
    HMGET_CMD = "TVS.HMGET"
    SCAN_CMD = "TVS.SCAN"

    def tvs_hset(
        self,
        index: str,
        key: str,
        vector: Union[VectorType, str] = None,
        is_binary=False,
        **kwargs
    ):
        """
        add/update a data entry to index
          @index: index name
          @key: key for the data entry
          @vector: optional, vector value of the data entry
          @is_binary: whether @vector is a binary vector
          @kwargs: optional, attribute pairs for the data entry
        """
        attributes = reduce(lambda x, y: x + y, kwargs.items(), ())
        if vector is None:
            return self.execute_command(self.HSET_CMD, index, key, *attributes)
        if not isinstance(vector, str):
            vector = TairVectorCommands.encode_vector(vector, is_binary)
        return self.execute_command(
            self.HSET_CMD, index, key, Constants.VECTOR_KEY, vector, *attributes
        )

    def tvs_del(self, index: str, key: str):
        """
        delete a data entry from index
        """
        return self.execute_command(self.DEL_CMD, index, key)

    def tvs_hdel(self, index: str, key: str, *args):
        """
        delete attribute pairs for a data entry
        """
        if len(args) == 0:
            # nothing to delete
            return 0
        return self.execute_command(self.HDEL_CMD, index, key, *args)

    def tvs_hgetall(self, index: str, key: str):
        """
        get the vector value(if any) and attributes(if any) for a data entry
        """
        return self.execute_command(self.HGETALL_CMD, index, key)

    def tvs_hmget(self, index: str, key: str, *args):
        """
        get specified attributes of a data entry, use attribute key "VECTOR" to get vector value
        """
        return self.execute_command(self.HMGET_CMD, index, key, *args)

    # def tvs_hmget(self, index: str, key: str,fields: Iterable[str]):
    #     return self.execute_command(self.HMGET_CMD, index,key, *fields)

    def tvs_scan(self, index: str, pattern: str = None, batch: int = 10):
        """
        scan all data entries in an index
        """
        args = ([] if pattern is None else ["MATCH", pattern]) + ["COUNT", batch]
        get_batch = lambda c: self.execute_command(self.SCAN_CMD, index, c, *args)

        return TairVectorScanResult(self, get_batch)

    SEARCH_CMD = "TVS.KNNSEARCH"
    MSEARCH_CMD = "TVS.MKNNSEARCH"

    def tvs_knnsearch(
        self,
        index: str,
        k: int,
        vector: Union[VectorType, str],
        is_binary: bool = False,
        filter_str: str = None,
        **kwargs
    ):
        """
        search for the top @k approximate nearest neighbors of @vector in an index
        """
        params = reduce(lambda x, y: x + y, kwargs.items(), ())
        if not isinstance(vector, str):
            vector = TairVectorCommands.encode_vector(vector, is_binary)
        if filter_str is None:
            return self.execute_command(self.SEARCH_CMD, index, k, vector, *params)
        return self.execute_command(
            self.SEARCH_CMD, index, k, vector, filter_str, *params
        )

    def tvs_mknnsearch(
        self,
        index: str,
        k: int,
        vectors: Sequence[VectorType],
        is_binary: bool = False,
        filter_str: str = None,
        **kwargs
    ):
        """
        batch approximate nearest neighbors search for a list of vectors
        """
        params = reduce(lambda x, y: x + y, kwargs.items(), ())
        encoded_vectors = [
            TairVectorCommands.encode_vector(x, is_binary) for x in vectors
        ]
        if filter_str is None:
            return self.execute_command(
                self.MSEARCH_CMD,
                index,
                k,
                len(encoded_vectors),
                *encoded_vectors,
                *params
            )
        return self.execute_command(
            self.MSEARCH_CMD,
            index,
            k,
            len(encoded_vectors),
            *encoded_vectors,
            filter_str,
            *params
        )

def parse_tvs_get_index_result(resp) -> Union[Dict, None]:
    if len(resp) == 0:
        return None
    return pairs_to_dict(resp, decode_keys=True, decode_string_values=True)

def parse_tvs_get_result(resp) -> Dict:
    result = pairs_to_dict(resp, decode_keys=True, decode_string_values=False)

    if Constants.VECTOR_KEY in result:
        result[Constants.VECTOR_KEY] = TextVectorEncoder.decode(
            result[Constants.VECTOR_KEY]
        )
    values = map(str_if_bytes, result.values())
    return dict(zip(result.keys(), values))

def parse_tvs_hmget_result(resp) -> tuple:
    if len(resp) == 0:
        return None
    return ([resp[i].decode("ascii") if resp[i] else None for i in range(0, len(resp))])

def parse_tvs_search_result(resp) -> List[Tuple]:
    return [(resp[i], float(resp[i + 1])) for i in range(0, len(resp), 2)]

def parse_tvs_msearch_result(resp) -> List[List[Tuple]]:
    return [parse_tvs_search_result(r) for r in resp]