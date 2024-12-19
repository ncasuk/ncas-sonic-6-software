#!/bin/bash

DIR=$(dirname "$(readlink -f "$0")")

# create python venv and install requirements
python3 -m venv ${DIR}/../venv
source ${DIR}/../venv/bin/activate
pip install -r ${DIR}/../proc_netcdf/requirements.txt

