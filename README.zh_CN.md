[English](README.md) | 简体中文

# tair-py

[![Test](https://github.com/alibaba/tair-py/actions/workflows/test.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/test.yml)
[![Format](https://github.com/alibaba/tair-py/actions/workflows/format.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/format.yml)
[![Coverage](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

基于 [redis-py](https://github.com/redis/redis-py) 封装的，[云数据库 Redis 企业版（Tair）](https://help.aliyun.com/document_detail/145956.html)的 Python 客户端。支持 Tair 的以下模块：

- [TairString](https://help.aliyun.com/document_detail/145902.html)
- [TairHash](https://help.aliyun.com/document_detail/145970.html)
- [TairZset](https://help.aliyun.com/document_detail/292812.html)

## 安装

从代码安装：

```shell
git clone https://github.com/alibaba/tair-py.git
cd tair-py
python setup.py install
```

## 用法

tair-py 支持 Python 3.7 及以上版本。

```pycon
>>> import tair
>>> t = tair.Tair(host='localhost', port=6379, db=0)
>>> t.exset('foo', 'bar')
b'OK'
>>> t.exget('foo')
[b'bar', 1]
```

## 维护者

[@Vincil Lau](https://github.com/VincilLau)。

## 如何贡献

非常欢迎你的加入！[提一个 Issue](https://github.com/alibaba/tair-py/issues/new) 或者提交一个 Pull Request。

## 使用许可

[MIT](LICENSE) © Alibaba
