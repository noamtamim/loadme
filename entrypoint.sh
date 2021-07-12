#!/usr/bin/env bash

# env

set -ex

python -c "from urllib.request import urlretrieve; urlretrieve('$1', 'script.py')"

shift

export PYTHONUNBUFFERED=1

python script.py "$@"
