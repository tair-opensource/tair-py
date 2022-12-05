# tair-py

[![Test](https://github.com/alibaba/tair-py/actions/workflows/test.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/test.yml)
[![Format](https://github.com/alibaba/tair-py/actions/workflows/format.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/format.yml)
[![Coverage](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![pypi](https://badge.fury.io/py/tair.svg)](https://pypi.org/project/tair/)

English | [简体中文](https://github.com/alibaba/tair-py/blob/main/README.zh_CN.md)

tair-py is a Python client of [Tair](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/apsaradb-for-redis-enhanced-edition-overview) based on [redis-py](https://github.com/redis/redis-py). The following modules of Tair are supported.

- [TairString](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairstring-commands), is a string that contains a version number. ([Open sourced](https://github.com/alibaba/TairString))
- [TairHash](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairhash-commands), is a hash that allows you to specify the expiration time and version number of a field. ([Open sourced](https://github.com/alibaba/TairHash))
- [TairZset](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairzset-commands), allows you to sort data of the double type based on multiple dimensions. ([Open sourced](https://github.com/alibaba/TairZset))
- [TairBloom](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairbloom-commands), is a Bloom filter that supports dynamic scaling. (Coming soon)
- [TairRoaring](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairroaring-commands), is a more efficient and balanced type of compressed bitmaps recognized by the industry. (Coming soon)
- [TairSearch](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairsearch-command), is a full-text search module developed in-house based on Redis modules. (Coming soon)
- [TairDoc](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairdoc-commands), to perform create, read, update, and delete (CRUD) operations on JSON data. (Coming soon)
- [TairGis](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairgis-commands), allowing you to query points, linestrings, and polygons. (Coming soon)
- [TairTs](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairts-commands), is a time series data structure that is developed on top of Redis modules.  (Coming soon)
- [TairCpc](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/taircpc-commands), is a data structure developed based on the compressed probability counting (CPC) sketch. (Coming soon)
- [TairVector](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairvector),  is a self-developed data structure that provides high-performance real-time storage and retrieval of vectors. (Coming soon)

## Install

Install from pip:

```shell
pip install tair
```

Install from source:

```shell
git clone https://github.com/alibaba/tair-py.git
cd tair-py
python setup.py install
```

## Usage

tair-py supports Python 3.7+.

```python
#!/usr/bin/env python

from tair import Tair

if __name__ == "__main__":
    try:
        t = Tair(host="localhost", port=6379, db=0)
        t.exset("foo", "bar")
        # exget return a ExgetResult object.
        ret = t.exget("foo")
        print(ret.value)  # output b'bar'.
        print(ret.version)  # output 1
    except Exception as e:
        print(e)
        exit(1)
```

For more examples, please see [examples](https://github.com/alibaba/tair-py/blob/main/examples).

## Maintainers

[@Vincil Lau](https://github.com/VincilLau).

## Contributing

Feel free to dive in! [Open an issue](https://github.com/alibaba/tair-py/issues/new) or submit a Pull Request.

## License

[MIT](LICENSE)

## Tair All SDK

| language | GitHub |
|----------|---|
| Java     |https://github.com/alibaba/alibabacloud-tairjedis-sdk|
| Python   |https://github.com/alibaba/tair-py|
| Go       |https://github.com/alibaba/tair-go|
| .Net     |https://github.com/alibaba/AlibabaCloud.TairSDK|