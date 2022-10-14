#!/usr/bin/env python

from tair import ResponseError

from conf_examples import get_tair


# bargainRush decrements the value of key from upperBound by 1 until lowerBound
# @param key the key
# @param upperBound the max value
# @param lowerBound the min value
# @return acquire success: true; fail: false
def bargain_rush(key: str, upper_bound: int, lower_bound: int) -> bool:
    try:
        tair = get_tair()
        tair.exincrby(key, -1, minval=lower_bound, define=upper_bound)
        return True
    except ResponseError as e:
        print(e)
        return False


if __name__ == "__main__":
    key = "bargainRush"
    for i in range(20):
        print("attempt %d, result: %s" % (i, bargain_rush(key, 10, 0)))
