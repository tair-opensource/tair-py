#!/usr/bin/env python

import logging
import time
from threading import Thread
from typing import List

from tair import ResponseError
from tair.cluster import TairCluster as Tair

# change the following configuration for your Tair.
TAIR_HOST = "localhost"
TAIR_PORT = 30001
TAIR_USERNAME = None
TAIR_PASSWORD = None

tair: Tair = Tair(
    host=TAIR_HOST,
    port=TAIR_PORT,
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


def thread_target(user_id) -> None:
    while purchase(user_id):
        time.sleep(1)


if __name__ == "__main__":
    logger = logging.getLogger("redis.cluster")
    logger.setLevel(logging.CRITICAL)

    if not init():
        exit()
    threads: List[Thread] = []
    for i in range(10):
        t = Thread(target=thread_target, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
