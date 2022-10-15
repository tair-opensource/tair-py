#!/usr/bin/env python

import time
import uuid
from threading import Thread
from typing import List

from conf_examples import get_tair

from tair import TairError

LOCK_KEY: str = "LOCK_KEY"


class Account:
    def __init__(self, balance: int) -> None:
        self.balance = balance


# try_lock locks atomically via set with NX flag
# request_id prevents the lock from being deleted by mistake
# expire_time is to prevent the deadlock of business machine downtime
def try_lock(key: str, request_id: str, expire_time: int) -> bool:
    try:
        tair = get_tair()
        result = tair.set(key, request_id, ex=expire_time, nx=True)
        # if the command was successful, return True
        # else return None
        return result is not None
    except TairError as e:
        print(e)
        return False


# release_lock atomically releases the lock via the CAD command
# request_id ensures that the released lock is added by itself
def release_lock(key: str, request_id: str) -> bool:
    try:
        tair = get_tair()
        result = tair.cad(key, request_id)
        # if the key doesn't exist, return -1
        # if the request_id doesn't match, return 0
        # else return 1
        return result == 1
    except TairError as e:
        print(e)
        return False


def deposit_and_withdraw(account: Account):
    request_id = str(uuid.uuid4())
    if try_lock(LOCK_KEY, request_id, 2):
        print(f"balance: {account.balance}")
        if account.balance != 10:
            raise RuntimeError(
                f"balance should not be negative value: {account.balance}"
            )
        account.balance += 1000
        time.sleep(1)
        account.balance -= 1000
        release_lock(LOCK_KEY, request_id)
    time.sleep(1)


def thread_target(account: Account):
    while True:
        deposit_and_withdraw(account)


if __name__ == "__main__":
    account = Account(10)
    threads: List[Thread] = []
    for i in range(10):
        t = Thread(target=thread_target, args=(account,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
