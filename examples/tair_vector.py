#!/usr/bin/env python
from conf_examples import get_tair
from tair import ResponseError

# create an index
# @param index_name the name of index
# @param dims the dimension of vector
# @return success: True, fail: False.
def create_index(index_name: str, dims: str):
    try:
        tair = get_tair()
        index_params = {
            "M": 32,
            "ef_construct": 200,
        }
        #index_params the params of index
        return tair.tvs_create_index(index_name, dims,**index_params)
    except ResponseError as e:
        print(e)
        return None

# delete an index
# @param index_name the name of index
# @return success: True, fail: False.
def delete_index(index_name: str):
    try:
        tair = get_tair()
        return tair.tvs_del_index(index_name)
    except ResponseError as e:
        print(e)
        return False


if __name__ == "__main__":
    create_index("test",4)
    delete_index("test")