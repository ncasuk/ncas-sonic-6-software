#!/bin/bash

DIR=$(dirname "$(readlink -f "$0")")

# create python venv and install requirements
python3 -m venv ${DIR}/../venv
# if previous command fails, exit
if [ $? -ne 0 ]; then
    echo "Failed to create python venv"
    exit 1
fi
source ${DIR}/../venv/bin/activate
pip install -r ${DIR}/../proc_netcdf/requirements.txt

