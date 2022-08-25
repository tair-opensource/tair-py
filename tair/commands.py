from typing import Union

from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from tair.tairbloom import TairBloomCommands
from tair.tairdoc import TairDocCommands
from tair.tairgis import TairGisCommands
from tair.tairhash import (
    TairHashCommands,
    parse_exhgetall,
    parse_exhgetwithver,
    parse_exhincrbyfloat,
    parse_exhmgetwithver,
    parse_exhscan,
)
from tair.tairroaring import TairRoaringCommands, parse_tr_scan
from tair.tairsearch import ScandocidResult, TairSearchCommands
from tair.tairstring import (
    TairStringCommands,
    parse_excas,
    parse_exget,
    parse_exincrbyfloat,
    parse_exset,
)
from tair.tairts import TairTsCommands
from tair.tairzset import TairZsetCommands, parse_tair_zset_items


class TairCommands(
    TairHashCommands,
    TairStringCommands,
    TairZsetCommands,
    TairBloomCommands,
    TairRoaringCommands,
    TairSearchCommands,
    TairGisCommands,
    TairDocCommands,
    TairTsCommands,
):
    pass


def str_if_bytes(value: Union[str, bytes]) -> str:
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


def bool_ok(resp) -> bool:
    return str_if_bytes(resp) == "OK"


TAIR_RESPONSE_CALLBACKS = {
    # TairString
    "EXSET": parse_exset,
    "EXGET": parse_exget,
    "EXINCRBYFLOAT": parse_exincrbyfloat,
    "EXCAS": parse_excas,
    # TairHash
    "EXHMSET": bool_ok,
    "EXHINCRBYFLOAT": parse_exhincrbyfloat,
    "EXHGETWITHVER": parse_exhgetwithver,
    "EXHMGETWITHVER": parse_exhmgetwithver,
    "EXHGETALL": parse_exhgetall,
    "EXHSCAN": parse_exhscan,
    # TairZset
    "EXZINCRBY": str_if_bytes,
    "EXZSCORE": str_if_bytes,
    "EXZRANGE": parse_tair_zset_items,
    "EXZREVRANGE": parse_tair_zset_items,
    "EXZRANGEBYSCORE": parse_tair_zset_items,
    "EXZREVRANGEBYSCORE": parse_tair_zset_items,
    # TairBloom
    "BF.RESERVE": bool_ok,
    # TairRoaring
    "TR.GETBIT": lambda resp: resp if resp == 1 else 0,
    "TR.APPENDINTARRAY": bool_ok,
    "TR.SETINTARRAY": bool_ok,
    "TR.SETBITARRAY": bool_ok,
    "TR.OPTIMIZE": bool_ok,
    "TR.SCAN": parse_tr_scan,
    "TR.RANGEBITARRAY": lambda resp: resp.decode(),
    "TR.JACCARD": lambda resp: float(resp.decode()),
    # TairSearch
    "TFT.CREATEINDEX": bool_ok,
    "TFT.UPDATEINDEX": bool_ok,
    "TFT.GETINDEX": lambda resp: None if resp is None else resp.decode(),
    "TFT.ADDDOC": lambda resp: resp.decode(),
    "TFT.MADDDOC": bool_ok,
    "TFT.DELDOC": lambda resp: int(resp.decode()),
    "TFT.UPDATEDOCFIELD": bool_ok,
    "TFT.INCRFLOATDOCFIELD": lambda resp: float(resp.decode()),
    "TFT.GETDOC": lambda resp: None if resp is None else resp.decode(),
    "TFT.SCANDOCID": lambda resp: ScandocidResult(
        resp[0].decode(), [i.decode() for i in resp[1]]
    ),
    "TFT.DELALL": bool_ok,
    "TFT.SEARCH": lambda resp: resp.decode(),
    "TFT.GETSUG": lambda resp: [i.decode() for i in resp],
    "TFT.GETALLSUGS": lambda resp: [i.decode() for i in resp],
    # TairDoc
    "JSON.SET": lambda resp: None if resp is None else resp == b"OK",
    "JSON.TYPE": str_if_bytes,
    # TairTs
    "EXTS.P.CREATE": bool_ok,
    "EXTS.S.CREATE": bool_ok,
    "EXTS.S.ALTER": bool_ok,
    "EXTS.S.ADD": bool_ok,
    "EXTS.S.MADD": lambda resp: [bool_ok(i) for i in resp],
    "EXTS.S.INCRBY": bool_ok,
    "EXTS.S.MINCRBY": lambda resp: [bool_ok(i) for i in resp],
    "EXTS.S.DEL": bool_ok,
    "EXTS.S.RAW_MODIFY": bool_ok,
    "EXTS.S.RAW_MMODIFY": lambda resp: [bool_ok(i) for i in resp],
    "EXTS.S.RAW_INCRBY": bool_ok,
    "EXTS.S.RAW_MINCRBY": lambda resp: [bool_ok(i) for i in resp],
}


def set_tair_response_callback(redis: Union[Redis, AsyncRedis]):
    for cmd, cb in TAIR_RESPONSE_CALLBACKS.items():
        redis.set_response_callback(cmd, cb)
