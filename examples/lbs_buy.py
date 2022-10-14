#!/usr/bin/env python

from tair import ResponseError

from conf_examples import get_tair


# Add a service store geographical scope.
# @param key the key
# @param storeName the store name
# @param storeWkt the store wkt
# @return success: true, fail: false
def add_polygon(key: str, store_name: str, store_wkt) -> bool:
    try:
        tair = get_tair()
        ret = tair.gis_add(key, {store_name: store_wkt})
        return ret == 1
    except ResponseError as e:
        print(e)
        return False


# Determine whether the user's location is within the service range of the store.
# @param key the key
# @param userLocation the user location
# @return Stores that can serve users
def get_service_store(key: str, user_location: str):
    try:
        tair = get_tair()
        return tair.gis_contains(key, user_location)
    except:
        return None


if __name__ == "__main__":
    key = "LbsBuy"
    add_polygon(
        key,
        "store-1",
        "POLYGON ((120.058897 30.283681, 120.093033 30.286363, 120.097632 30.269147, 120.050705 30.252863))",
    )
    add_polygon(
        key,
        "store-2",
        "POLYGON ((120.026343 30.285739, 120.029289 30.280749, 120.0382 30.281997, 120.037051 30.288109))",
    )
    print(get_service_store(key, "POINT(120.072264 30.27501)"))
