[English](https://github.com/alibaba/tair-py/blob/main/README.md) | 简体中文

# tair-py

[![Test](https://github.com/alibaba/tair-py/actions/workflows/test.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/test.yml)
[![Format](https://github.com/alibaba/tair-py/actions/workflows/format.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/format.yml)
[![Coverage](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml/badge.svg)](https://github.com/alibaba/tair-py/actions/workflows/coverage.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![pypi](https://badge.fury.io/py/tair.svg)](https://pypi.org/project/tair/)

基于 [redis-py](https://github.com/redis/redis-py) 封装的，[云数据库 Redis 企业版（Tair）](https://help.aliyun.com/document_detail/145956.html)的 Python 客户端。支持 Tair 的以下模块：

- [TairString](https://help.aliyun.com/document_detail/145902.html), 支持 string 设置 version，增强的 `cas` 和 `cad` 命令可轻松实现分布式锁。([已开源](https://github.com/alibaba/TairString))
- [TairHash](https://help.aliyun.com/document_detail/145970.html), 可实现 field 级别的过期。([已开源](https://github.com/alibaba/TairHash))
- [TairZset](https://help.aliyun.com/document_detail/292812.html), 支持多维排序。([已开源](https://github.com/alibaba/TairZset))
- [TairBloom](https://help.aliyun.com/document_detail/145972.html), 支持动态扩容的布隆过滤器。（待开源）
- [TairRoaring](https://help.aliyun.com/document_detail/311433.html), Roaring Bitmap, 使用少量的存储空间来实现海量数据的查询优化。（待开源）
- [TairSearch](https://help.aliyun.com/document_detail/417908.html), 支持ES-LIKE语法的全文索引和搜索模块。（待开源） 
- [TairGis](https://help.aliyun.com/document_detail/145971.html), 支持地理位置点、线、面的相交、包含等关系判断。（待开源）
- [TairDoc](https://help.aliyun.com/document_detail/145940.html), 支持存储`JSON`类型。（待开源）
- [TairTs](https://help.aliyun.com/document_detail/408954.html), 时序数据结构，提供低时延、高并发的内存读写访问。（待开源）
- [TairCpc](https://help.aliyun.com/document_detail/410587.html), 基于CPC（Compressed Probability Counting）压缩算法开发的数据结构，支持仅占用很小的内存空间对采样数据进行高性能计算。（待开源）

## 安装

从 pip 安装：

```shell
pip install tair
```

从代码安装：

```shell
git clone https://github.com/alibaba/tair-py.git
cd tair-py
python setup.py install
```

## 用法

tair-py 支持 Python 3.7 及以上版本。

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

更多例子请查看 [examples](https://github.com/alibaba/tair-py/blob/main/examples).

## 维护者

[@Vincil Lau](https://github.com/VincilLau)。

## 如何贡献

非常欢迎你的加入！[提一个 Issue](https://github.com/alibaba/tair-py/issues/new) 或者提交一个 Pull Request。

## 使用许可

[MIT](LICENSE)

# Tair 所有的 SDK

| language | GitHub |
|----------|---|
| Java     |https://github.com/alibaba/alibabacloud-tairjedis-sdk|
| Python   |https://github.com/alibaba/tair-py|
| Go       |https://github.com/alibaba/tair-go|
| .Net     |https://github.com/alibaba/AlibabaCloud.TairSDK|