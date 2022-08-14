#!/usr/bin/env python

import asyncio
import uuid
from tair import TairError
from tair.asyncio import Tair

# change the following configuration for your Tair.
TAIR_HOST = "localhost"
TAIR_PORT = 6379
TAIR_DB = 0
TAIR_USERNAME = None
TAIR_PASSWORD = None

tair = None


async def init():
    global tair

    tair = Tair(
        host=TAIR_HOST,
        port=TAIR_PORT,
        db=TAIR_DB,
        username=TAIR_USERNAME,
        password=TAIR_PASSWORD,
    )
    await tair.initialize()


LOCK_KEY: str = "LOCK_KEY"


class Account:
    def __init__(self, balance: int) -> None:
        self.balance = balance


# try_lock locks atomically via set with NX flag
# request_id prevents the lock from being deleted by mistake
# expire_time is to prevent the deadlock of business machine downtime
async def try_lock(key: str, request_id: str, expire_time: int) -> bool:
    try:
        result = await tair.set(key, request_id, ex=expire_time, nx=True)
        # if the command was successful, return True
        # else return None
        return result is not None
    except TairError as e:
        print(e)
        return False


# release_lock atomically releases the lock via the CAD command
# request_id ensures that the released lock is added by itself
async def release_lock(key: str, request_id: str) -> bool:
    try:
        result = await tair.cad(key, request_id)
        # if the key doesn't exist, return -1
        # if the request_id doesn't match, return 0
        # else return 1
        return result == 1
    except TairError as e:
        print(e)
        return False


async def deposit_and_withdraw(account: Account) -> None:
    request_id = str(uuid.uuid4())
    if await try_lock(LOCK_KEY, request_id, 2):
        print(f"balance: {account.balance}")
        if account.balance != 10:
            raise RuntimeError(
                f"balance should not be negative value: {account.balance}"
            )
        account.balance += 1000
        await asyncio.sleep(1)
        account.balance -= 1000
        await release_lock(LOCK_KEY, request_id)
    await asyncio.sleep(1)


async def task_func(account: Account) -> None:
    while True:
        await deposit_and_withdraw(account)


async def main():
    await init()
    account = Account(10)
    tasks = []
    for i in range(10):
        task = asyncio.create_task(task_func(account))
        tasks.append(task)
    for task in tasks:
        await task


if __name__ == "__main__":
    asyncio.run(main())
