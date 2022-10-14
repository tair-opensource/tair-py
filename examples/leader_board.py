#!/usr/bin/env python

from typing import Union

from tair import ResponseError

from conf_examples import get_tair


# Add User with Multi scores.
# @param key the key
# @param member the member
# @param scores the multi dimensional score
# @return success: true; fail: false
def add_user(key: str, member: str, score: Union[float, str]) -> bool:
    try:
        tair = get_tair()
        tair.exzadd(key, {member: score})
        return True
    except:
        return False


# Get the top element of the leaderboard.
# @param key the key
# @param startOffset start offset
# @param endOffset end offset
# @return the top elements.
def top(key: str, start_offset: int, end_offset: int) -> list:
    try:
        tair = get_tair()
        return tair.exzrevrange(key, start_offset, end_offset, withscores=True)
    except ResponseError as e:
        print(e)
        return []


if __name__ == "__main__":
    key = "LeaderBoard"
    # add three user
    add_user(key, "user1", "20#10#30")
    add_user(key, "user2", "20#15#10")
    add_user(key, "user3", "30#10#10")
    # get top 2
    for i in top(key, 0, 1):
        print(i)
