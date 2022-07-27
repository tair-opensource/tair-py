#!/usr/bin/env python
from setuptools import setup

setup(
    name="tair",
    description="Python client for Tair",
    version="0.0.1",
    license="MIT",
    url="https://github.com/alibaba/tair-py",
    author="Vincil Lau",
    author_email="vincillau@outlook.com",
    python_requires=">=3.7",
    packages=["tair", "tair.asyncio"],
    install_requires=["redis >= 4.2"],
)
