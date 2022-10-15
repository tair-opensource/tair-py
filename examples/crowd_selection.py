#!/usr/bin/env python

from typing import List

from conf_examples import get_tair

from tair import ResponseError


# Set key offset value, value can be 0 or 1.
# @param key the key
# @param offset the offset
# @param value the new value
# @return success: true, fail: false
def setbit(key: str, offset: int, value: int) -> bool:
    try:
        tair = get_tair()
        tair.tr_setbit(key, offset, value)
        return True
    except ResponseError as e:
        print(e)
        return False


# Get key offset value.
# @param key the key
# @param offset the offset
# @return the offset value, if not exists, return 0
def getbit(key: str, offset: int) -> int:
    try:
        tair = get_tair()
        return tair.tr_getbit(key, offset)
    except:
        return -1


# AND the two bitmaps and store the result in a new destkey.
# @param destkey the dest key
# @param keys the source key
# @return success: true, fail: false
def bitand(destkey: str, keys: List[str]) -> bool:
    try:
        tair = get_tair()
        tair.tr_bitop(destkey, "AND", keys)
        return True
    except ResponseError as e:
        print(e)
        return False


if __name__ == "__main__":
    key1 = "CrowdSelection-1"
    key2 = "CrowdSelection-2"
    key3 = "CrowdSelection-destKey"
    setbit(key1, 0, 1)
    setbit(key1, 1, 1)
    setbit(key2, 1, 1)
    print(getbit(key1, 0))
    bitand(key3, [key1, key2])
    print(getbit(key3, 0))
    print(getbit(key3, 1))
