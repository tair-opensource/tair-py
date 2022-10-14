#!/usr/bin/env python

from tair import ResponseError

from conf_examples import get_tair


# Add equipment to package
# @param key the key
# @param packagePath the package path
# @param equipment the new equipment
# @return total number of equipment
def add_equipment(key: str, package_path: str, equipment: str) -> int:
    try:
        tair = get_tair()
        return tair.json_arrappend(key, package_path, equipment)
    except ResponseError as e:
        print(e)
        return -1


if __name__ == "__main__":
    key = "GamePackage"
    tair = get_tair()
    tair.json_set(key, ".", "[]")
    print(add_equipment(key, ".", ['"lightsaber"']))
    print(add_equipment(key, ".", ['"howitzer"']))
    print(add_equipment(key, ".", ['"gun"']))
    print(tair.json_get(key, ".").decode())
