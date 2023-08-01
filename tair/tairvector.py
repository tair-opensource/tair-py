from concurrent.futures import ThreadPoolExecutor
from functools import partial, reduce
from typing import Dict, List, Sequence, Tuple, Union, Optional, Iterable
from tair.typing import AbsExpiryT, CommandsProtocol, ExpiryT, ResponseT

from redis.client import pairs_to_dict
from redis.utils import str_if_bytes

VectorType = Sequence[Union[int, float]]


class DistanceMetric:
    Euclidean = "L2"  # an alias to L2
    L2 = "L2"
    InnerProduct = "IP"
    Jaccard = "JACCARD"
    Cosine = "COSINE"


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

    @classmethod
    def encode(cls, vector: VectorType, is_binary=False) -> bytes:
        s = ""
        if is_binary:
            s = "[" + ",".join([cls.BITS[x] for x in vector]) + "]"
        else:
            s = "[" + ",".join(["%f" % x for x in vector]) + "]"
        return bytes(s, encoding="ascii")  # ascii is enough

    @classmethod
    def decode(cls, buf: bytes) -> Tuple[float]:
        if buf[0] != ord("[") or buf[-1] != ord("]"):
            raise ValueError("invalid text vector value")
        is_int = True
        components = buf[1:-1].split(cls.SEP)
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
        return ret

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

    def tvs_hset(self, key: str, vector: Union[VectorType, str, None] = None, **kwargs):
        """add/update a data entry to index
        @key: key for the data entry
        @vector: optional, vector value of the data entry
        @kwargs: optional, attribute pairs for the data entry
        """
        return self.client.tvs_hset(self.name, key, vector, self.is_binary, **kwargs)

    def tvs_knnsearch(
            self,
            k: int,
            vector: Union[VectorType, str],
            filter_str: Optional[str] = None,
            **kwargs
    ):
        """search for the top @k approximate nearest neighbors of @vector"""
        return self.client.tvs_knnsearch(
            self.name, k, vector, self.is_binary, filter_str, **kwargs
        )

    def tvs_mknnsearch(
            self,
            k: int,
            vectors: Sequence[VectorType],
            filter_str: Optional[str] = None,
            **kwargs
    ):
        """batch approximate nearest neighbors search for a list of vectors"""
        return self.client.tvs_mknnsearch(
            self.name, k, vectors, self.is_binary, filter_str, **kwargs
        )

    def __str__(self):
        return "%s[%s]" % (self.name, self.params)

    def __repr__(self):
        return str(self)


class TairVectorCommands(CommandsProtocol):
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
            self, pattern: Optional[str] = None, batch: int = 10
    ) -> TairVectorScanResult:
        """
        scan all the indices
        """
        args = ([] if pattern is None else ["MATCH", pattern]) + ["COUNT", batch]

        def get_batch(c):
            return self.execute_command(self.SCAN_INDEX_CMD, c, *args)

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
            vector: Union[VectorType, str, None] = None,
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

    def tvs_scan(
            self,
            index: str,
            pattern: Optional[str] = None,
            batch: int = 10,
            filter_str: Optional[str] = None,
            vector: Optional[VectorType] = None,
            max_dist: Optional[float] = None,
    ):
        """
        scan all data entries in an index
        """
        args = ([] if pattern is None else ["MATCH", pattern]) + ["COUNT", batch]
        if filter_str is not None:
            args.append("FILTER")
            args.append(filter_str)
        if vector is not None and max_dist is not None:
            args.append("VECTOR")
            args.append(self.encode_vector(vector))
            args.append("MAX_DIST")
            args.append(max_dist)
        elif vector is None and max_dist is None:
            pass
        else:
            raise ValueError("missing vector or max_dist")

        def get_batch(c):
            return self.execute_command(self.SCAN_CMD, index, c, *args)

        return TairVectorScanResult(self, get_batch)

    def _tvs_scan(
            self,
            index: str,
            cursor: int = 0,
            count: Optional[int] = None,
            pattern: Optional[str] = None,
            filter_str: Optional[str] = None,
            vector: Union[VectorType, bytes, None] = None,
            max_dist: Optional[float] = None,
    ):
        args = [] if pattern is None else ["MATCH", pattern]
        if count is not None:
            args += ("COUNT", count)
        if filter_str is not None:
            args.append("FILTER")
            args.append(filter_str)
        if vector is not None and max_dist is not None:
            args.append("VECTOR")
            args.append(
                vector if isinstance(vector, bytes) else self.encode_vector(vector)
            )
            args.append("MAX_DIST")
            args.append(max_dist)
        elif vector is None and max_dist is None:
            pass
        else:
            raise ValueError("missing vector or max_dist")
        return self.execute_command(self.SCAN_CMD, index, cursor, *args)

    SEARCH_CMD = "TVS.KNNSEARCH"
    MSEARCH_CMD = "TVS.MKNNSEARCH"
    MINDEXKNNSEARCH_CMD = "TVS.MINDEXKNNSEARCH"
    MINDEXMKNNSEARCH_CMD = "TVS.MINDEXMKNNSEARCH"

    def tvs_knnsearch(
            self,
            index: str,
            k: int,
            vector: Union[VectorType, str, bytes],
            is_binary: bool = False,
            filter_str: Optional[str] = None,
            **kwargs
    ):
        """
        search for the top @k approximate nearest neighbors of @vector in an index
        """
        params = reduce(lambda x, y: x + y, kwargs.items(), ())
        if (not isinstance(vector, str)) and (not isinstance(vector, bytes)):
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
            filter_str: Optional[str] = None,
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

    def tvs_mindexknnsearch(
            self,
            index: Sequence[str],
            k: int,
            vector: Union[VectorType, str, bytes],
            is_binary: bool = False,
            filter_str: Optional[str] = None,
            **kwargs
    ):
        """
        search for the top @k approximate nearest neighbors of @vector in indexs
        """
        params = reduce(lambda x, y: x + y, kwargs.items(), ())
        if (not isinstance(vector, str)) and (not isinstance(vector, bytes)):
            vector = TairVectorCommands.encode_vector(vector, is_binary)
        if filter_str is None:
            return self.execute_command(
                self.MINDEXKNNSEARCH_CMD, len(index), *index, k, vector, *params
            )
        return self.execute_command(
            self.MINDEXKNNSEARCH_CMD, len(index), *index, k, vector, filter_str, *params
        )

    def tvs_mindexmknnsearch(
            self,
            index: Sequence[str],
            k: int,
            vectors: Sequence[VectorType],
            is_binary: bool = False,
            filter_str: Optional[str] = None,
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
                self.MINDEXMKNNSEARCH_CMD,
                len(index),
                *index,
                k,
                len(encoded_vectors),
                *encoded_vectors,
                *params
            )
        return self.execute_command(
            self.MINDEXMKNNSEARCH_CMD,
            len(index),
            *index,
            k,
            len(encoded_vectors),
            *encoded_vectors,
            filter_str,
            *params
        )

    GETDISTANCE_CMD = "TVS.GETDISTANCE"

    def _tvs_getdistance(
            self,
            index_name: str,
            vector: VectorType,
            keys: Iterable[str],
            top_n: Optional[int] = None,
            max_dist: Optional[float] = None,
            filter_str: Optional[str] = None,
    ):
        """
        low level interface for TVS.GETDISTANCE
        """
        args = list(keys)
        if top_n is not None:
            args += ("TOPN", top_n)
        if max_dist is not None:
            args += ("MAX_DIST", max_dist)
        if filter_str is not None:
            args += ("FILTER", filter_str)

        if (not isinstance(vector, str)) and (not isinstance(vector, bytes)):
            vector_str = self.encode_vector(vector)
        else:
            vector_str = vector
        return self.execute_command(
            self.GETDISTANCE_CMD, index_name, vector_str, len(keys), *args
        )

    def tvs_getdistance(
            self,
            index_name: str,
            vector: Union[VectorType, str, bytes],
            keys: Iterable[str],
            batch_size: int = 100000,
            parallelism: int = 1,
            top_n: Optional[int] = None,
            max_dist: Optional[float] = None,
            filter_str: Optional[str] = None,
    ):
        """
        wrapped interface for TVS.GETDISTANCE
        """
        import heapq
        import itertools

        if (not isinstance(vector, str)) and (not isinstance(vector, bytes)):
            vector_str = self.encode_vector(vector)
        else:
            vector_str = vector

        k = len(keys)
        if top_n is not None:
            k = min(k, top_n)

        args = ["TOPN", k]
        if max_dist is not None:
            args += ("MAX_DIST", max_dist)
        if filter_str is not None:
            args += ("FILTER", filter_str)

        def process_batch(batch):
            return self.execute_command(
                self.GETDISTANCE_CMD,
                index_name,
                vector_str,
                len(batch),
                *(batch + args)
            )

        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            batches = [
                keys[i: i + batch_size] for i in range(0, len(keys), batch_size)
            ]

            futures = [executor.submit(process_batch, batch) for batch in batches]

            queue = []
            for f in futures:
                result = f.result()
                queue = heapq.merge(
                    queue,
                    [
                        (float(result[i + 1]), result[i])
                        for i in range(0, len(result), 2)
                    ],
                )
            queue = itertools.islice(queue, k)
            return [(key, score) for score, key in queue]

    HINCRBY_CMD = "TVS.HINCRBY"
    HINCRBYFLOAT_CMD = "TVS.HINCRBYFLOAT"

    def tvs_hincrby(self, index: str, key: str, field: str, num: int):
        """
        increment the long value of a tairvector field by the given amount, not support field VECTOR
        """
        return self.execute_command(self.HINCRBY_CMD, index, key, field, num)

    def tvs_hincrbyfloat(self, index: str, key: str, field: str, num: float):
        """
        increment the float value of a tairvector field by the given amount, not support field VECTOR
        """
        return self.execute_command(self.HINCRBYFLOAT_CMD, index, key, field, num)

    def tvs_hexpire(self, index: str, key: str, ex: ExpiryT) -> ResponseT:
        return self.execute_command("TVS.HEXPIRE", index, key, ex)

    def tvs_hexpireat(self, index: str, key: str, exat: AbsExpiryT) -> ResponseT:
        return self.execute_command("TVS.HEXPIREAT", index, key, exat)

    def tvs_hpexpire(self, index: str, key: str, px: ExpiryT) -> ResponseT:
        return self.execute_command("TVS.HPEXPIRE", index, key, px)

    def tvs_hpexpireat(self, index: str, key: str, pxat: AbsExpiryT) -> ResponseT:
        return self.execute_command("TVS.HPEXPIREAT", index, key, pxat)

    def tvs_httl(self, index: str, key: str) -> ResponseT:
        return self.execute_command("TVS.HTTL", index, key)

    def tvs_hpttl(self, index: str, key: str) -> ResponseT:
        return self.execute_command("TVS.HPTTL", index, key)

    def tvs_hexpiretime(self, index: str, key: str) -> ResponseT:
        return self.execute_command("TVS.HEXPIRETIME", index, key)

    def tvs_hpexpiretime(self, index: str, key: str) -> ResponseT:
        return self.execute_command("TVS.HPEXPIRETIME", index, key)


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


def parse_tvs_hmget_result(resp) -> Optional[Tuple]:
    if len(resp) == 0:
        return None
    return resp


def parse_tvs_search_result(resp) -> List[Tuple]:
    return [(resp[i], float(resp[i + 1])) for i in range(0, len(resp), 2)]


def parse_tvs_msearch_result(resp) -> List[List[Tuple]]:
    return [parse_tvs_search_result(r) for r in resp]


def parse_tvs_hincrbyfloat_result(resp) -> Union[float, None]:
    if resp is None:
        return resp
    return float(resp.decode())
