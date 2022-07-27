# tair-py

[![Test](https://github.com/alibaba/tair-py/actions/workflows/test.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/test.yml)
[![Format](https://github.com/alibaba/tair-py/actions/workflows/format.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/format.yml)
[![Coverage](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml)
[![pypi](https://badge.fury.io/py/tair.svg)](https://pypi.org/project/tair/)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

English | [简体中文](README.zh_CN.md)

tair-py is a Python client of [Tair](https://help.aliyun.com/document_detail/145956.html) based on [redis-py](https://github.com/redis/redis-py). The following modules of Tair are supported.

- [TairString](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairstring-commands)
- [TairHash](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairhash-commands)
- [TairZset](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairzset-commands)

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

```pycon
>>> import tair
>>> t = tair.Tair(host='localhost', port=6379, db=0)
>>> t.exset('foo', 'bar')
b'OK'
>>> t.exget('foo')
[b'bar', 1]
```

## Maintainers

[@Vincil Lau](https://github.com/VincilLau).

## Contributing

Feel free to dive in! [Open an issue](https://github.com/alibaba/tair-py/issues/new) or submit a Pull Request.

## License

[MIT](LICENSE)
