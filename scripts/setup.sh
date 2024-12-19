#!/bin/bash

DIR=$(dirname "$(readlink -f "$0")")

# create python mamba environment and install requirements
mamba env create -n sonic6 python=3.13 --file ${DIR}/../proc_netcdf/requirements.txt

# if previous command fails, exit
if [ $? -ne 0 ]; then
    echo "Failed to create python venv"
    exit 1
fi

