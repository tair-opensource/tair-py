#!/usr/bin/env python

import time

from tair import ResponseError

from conf_examples import get_tair


# Record the login time and device name of the device, and set the login status expiration time
# @param key the key
# @param loginTime the login time
# @param device the device name
# @param timeout the timeout
# @return success: true, fail: false
def device_login(key: str, login_time: str, device: str, timeout: int) -> bool:
    try:
        tair = get_tair()
        ret = tair.exhset(key, login_time, device, ex=timeout)
        return ret == 1
    except ResponseError as e:
        print(e)
        return False


def current_time_millis() -> int:
    return int(time.time() * 1000)


if __name__ == "__main__":
    key = "DeviceLogin"
    tair = get_tair()
    device_login(key, str(current_time_millis()), "device1", 2)
    device_login(key, str(current_time_millis()), "device2", 10)
    time.sleep(5)
    for i in tair.exhgetall(key):
        print(i)
