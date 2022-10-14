#!/usr/bin/env python

from conf_examples import get_tair

if __name__ == "__main__":
    tair = get_tair()
    key = "BitCount"
    tair.tr_setbit(key, 0, 1)
    tair.tr_setbit(key, 1, 1)
    tair.tr_setbit(key, 2, 1)
    print(tair.tr_bitcount(key))
