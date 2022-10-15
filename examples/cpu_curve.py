#!/usr/bin/env python

from conf_examples import get_tair

from tair import ResponseError


# add point to CPU_LOAD series
# @param ip machine ip
# @param ts the timestamp
# @param value the value
# @return success: true, fail: false.
def add_point(ip: str, ts: str, value: float) -> bool:
    try:
        tair = get_tair()
        return tair.exts_s_add("CPU_LOAD", ip, ts, value)
    except ResponseError as e:
        print(e)
        return False


# Range all data in a certain time series
# @param ip machine ip
# @param startTs start timestamp
# @param endTs end timestamp
# @return
def range_point(ip: str, start_ts: str, end_ts: str):
    try:
        tair = get_tair()
        return tair.exts_s_range("CPU_LOAD", ip, start_ts, end_ts)
    except ResponseError as e:
        print(e)
        return None


if __name__ == "__main__":
    add_point("127.0.0.1", "*", 10)
    add_point("127.0.0.1", "*", 20)
    add_point("127.0.0.1", "*", 30)
    range_point("127.0.0.1", "1587889046161", "*")
