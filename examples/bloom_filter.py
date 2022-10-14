#!/usr/bin/env python

import uuid

from conf_examples import get_tair


# Recommend the doc to the user, ignore it if it has been recommended, otherwise recommend it and mark it.
# @param userid the user id
# @param docid the doc id
def recommended_system(userid: str, docid: str) -> None:
    tair = get_tair()
    if tair.bf_exists(userid, docid):
        print(f"{docid} may exist in {userid}")
    else:
        # recommend to user sendRecommendMsg(docid);
        # add userid with docid
        tair.bf_add(userid, docid)
        print(f"{docid} does not exist in {userid}")


if __name__ == "__main__":
    key = "BloomFilter"
    value1 = str(uuid.uuid4())
    value2 = str(uuid.uuid4())
    recommended_system(key, value1)
    recommended_system(key, value1)
    recommended_system(key, value2)
