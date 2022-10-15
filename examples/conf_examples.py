#!/usr/bin/env python

from tair import Tair

# change the following configuration for your Tair.
TAIR_HOST = "vincillau.redis.rds.aliyuncs.com"
TAIR_PORT = 6379
TAIR_DB = 0
TAIR_USERNAME = "vincil"
TAIR_PASSWORD = "Hh2677634494+"


def get_tair() -> Tair:
    tair: Tair = Tair(
        host=TAIR_HOST,
        port=TAIR_PORT,
        db=TAIR_DB,
        username=TAIR_USERNAME,
        password=TAIR_PASSWORD,
    )
    return tair
