#!/usr/bin/env python

import time
from typing import List
from threading import Thread
from tair import Tair, ResponseError

# change the following configuration for your Tair.
TAIR_HOST = "localhost"
TAIR_PORT = 6379
TAIR_DB = 0
TAIR_USERNAME = None
TAIR_PASSWORD = None

tair: Tair = Tair(
    host=TAIR_HOST,
    port=TAIR_PORT,
    db=TAIR_DB,
    username=TAIR_USERNAME,
    password=TAIR_PASSWORD,
)


KEY = "COMMODITY_QUANTITY"


def init() -> bool:
    try:
        tair.exset(KEY, "100")
        return True
    except Exception as e:
        print(e)
        return False


def purchase(user_id) -> bool:
    try:
        ret = tair.exincrby(KEY, num=-1, minval=0)
        print(
            f"the user {user_id} has purchased an item, and currently there are {ret} left"
        )
        return True
    except ResponseError as e:
        print(f"the user {user_id} purchased failed: {e}")
        return False


def thread_target(user_id):
    while purchase(user_id):
        time.sleep(1)


if __name__ == "__main__":
    if not init():
        exit()
    threads: List[Thread] = []
    for i in range(10):
        t = Thread(target=thread_target, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
