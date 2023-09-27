#!/bin/bash

python -m isort --profile black tair/*.py tests/*.py
python -m isort -v --profile black tair/*.py tests/*.py