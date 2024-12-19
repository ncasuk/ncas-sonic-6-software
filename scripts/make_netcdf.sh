#!/bin/bash

DATE=$1

DIR=$(dirname "$(readlink -f "$0")")

# activate python environment
python_env=${DIR}/../venv/bin/activate
source $python_env

# check file exists
if [ ! -f /gws/pw/j07/ncas_obs_vol1/cvao/raw-data/ncas-sonic-6/data/${DATE}_ncas-sonic-6.csv ]; then
    echo "File not found!"
    exit 1
fi

python ${DIR}/../proc_netcdf/process_sonic.py /gws/pw/j07/ncas_obs_vol1/cvao/raw-data/ncas-sonic-6/data/20241127_longterm/${DATE:0:4}/${DATE:4:2}/${DATE}_ncas-sonic-6.csv -o /gws/pw/j07/ncas_obs_vol1/cvao/processing/20241127_longterm/${DATE:0:4}/${DATE:4:2} -m ${DIR}/../proc_netcdf/metadata.json