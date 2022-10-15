#!/usr/bin/env python

import time

from conf_examples import get_tair

from tair import ResponseError


# add longitude/latitude to key, timestamp represents the current moment.
# @param key the key
# @param ts the timestamp
# @param longitude the longitude
# @param latitude the latitude
# @return success: true, fail: false
def add_coordinate(key: str, ts: str, longitude: float, latitude: float) -> bool:
    try:
        tair = get_tair()
        ret = tair.gis_add(key, {ts: f"POINT ({longitude} {latitude})"})
        return ret == 1
    except ResponseError as e:
        print(e)
        return False


# Get all points under a key.
# @param key the key
# @return A map, the key is the time, and the value is the coordinate
def get_all_coordinate(key: str):
    try:
        tair = get_tair()
        return tair.gis_getall(key)
    except ResponseError as e:
        print(e)
        return None


def current_time_millis() -> int:
    return int(time.time() * 1000)


if __name__ == "__main__":
    key = "CarTrack"
    add_coordinate(key, str(current_time_millis()), 120.036188, 30.287922)
    time.sleep(1)
    add_coordinate(key, str(current_time_millis()), 120.037625, 30.292225)
    time.sleep(1)
    add_coordinate(key, str(current_time_millis()), 120.034435, 30.303303)
    for i in get_all_coordinate(key):
        print(i)
