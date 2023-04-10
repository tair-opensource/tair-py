#!/usr/bin/env python
from setuptools import setup

setup(
    name="tair",
    description="Python client for Tair",
    long_description=open("README.md").read().strip(),
    long_description_content_type="text/markdown",
    version="1.3.0",
    license="MIT",
    url="https://github.com/alibaba/tair-py",
    author="Vincil Lau",
    author_email="vincillau@outlook.com",
    python_requires=">=3.7",
    packages=["tair", "tair.asyncio"],
    install_requires=["redis >= 4.4.4"],
)
