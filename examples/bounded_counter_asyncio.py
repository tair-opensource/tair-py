#!/usr/bin/env python

import asyncio

from tair import ResponseError
from tair.asyncio import Tair

# change the following configuration for your Tair.
TAIR_HOST = "localhost"
TAIR_PORT = 6379
TAIR_DB = 0
TAIR_USERNAME = None
TAIR_PASSWORD = None

tair = None


KEY = "COMMODITY_QUANTITY"


async def init() -> bool:
    global tair

    tair = Tair(
        host=TAIR_HOST,
        port=TAIR_PORT,
        db=TAIR_DB,
        username=TAIR_USERNAME,
        password=TAIR_PASSWORD,
    )
    await tair.initialize()

    try:
        await tair.exset(KEY, "100")
        return True
    except Exception as e:
        print(e)
        return False


async def purchase(user_id) -> bool:
    try:
        ret = await tair.exincrby(KEY, num=-1, minval=0)
        print(
            f"the user {user_id} has purchased an item, and currently there are {ret} left"
        )
        return True
    except ResponseError as e:
        print(f"the user {user_id} purchased failed: {e}")
        return False


async def task_func(user_id):
    while await purchase(user_id):
        await asyncio.sleep(1)


async def main():
    if not await init():
        exit()
    tasks = []
    for i in range(10):
        task = asyncio.create_task(task_func(user_id=i))
        tasks.append(task)
    for task in tasks:
        await task


if __name__ == "__main__":
    asyncio.run(main())
