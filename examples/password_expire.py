#!/usr/bin/env python

import time

from conf_examples import get_tair

from tair import ResponseError


# Add a user and password with a timeout.
# @param key the key
# @param user the user
# @param password the password
# @param timeout the password expiration time
# @return success: true, fail: false
def add_user_pass(key: str, user: str, password: str, timeout: int) -> bool:
    try:
        tair = get_tair()
        ret = tair.exhset(key, user, password, ex=timeout)
        return ret == 1
    except ResponseError as e:
        print(e)
        return False


if __name__ == "__main__":
    key = "PasswordExpire"
    tair = get_tair()
    add_user_pass(key, "user1", "password1", 5)
    add_user_pass(key, "user2", "password2", 10)
    print("Wait 6 seconds")
    time.sleep(6)
    for i in tair.exhgetall(key):
        print(i)
