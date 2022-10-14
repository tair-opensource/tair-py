#!/usr/bin/env python

from conf_examples import get_tair

if __name__ == "__main__":
    tair = get_tair()

    key = "FraudPrevention"
    tair.cpc_update(key, "a")
    tair.cpc_update(key, "b")
    tair.cpc_update(key, "c")
    print(tair.cpc_estimate(key))
    tair.cpc_update(key, "d")
    print(tair.cpc_estimate(key))
