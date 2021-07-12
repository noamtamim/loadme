#!/usr/bin/env bash

# env

set -ex

# Avoid curl because it's not available on slim
python -c "from urllib.request import urlretrieve; urlretrieve('$1', 'script.py')"

shift

export PYTHONUNBUFFERED=1

python script.py "$@"
