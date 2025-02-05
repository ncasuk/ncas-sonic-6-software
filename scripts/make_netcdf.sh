#!/bin/bash

DATE=$1

DIR=$(dirname "$(readlink -f "$0")")

# check sonic file exists
if [ ! -f /gws/pw/j07/ncas_obs_vol1/cvao/raw_data/ncas-sonic-6/data/20241127_longterm/${DATE:0:4}/${DATE:4:2}/${DATE}_ncas-sonic-6.csv ]; then
    echo "File not found!"
    exit 1
fi

mkdir -p /gws/pw/j07/ncas_obs_vol1/cvao/processing/ncas-sonic-6/20241127_longterm/${DATE:0:4}/${DATE:4:2}

# if aws 7 file exists, pass it to python, if not then don't
if [ -f /gws/pw/j07/ncas_obs_vol1/cvao/raw_data/ncas-aws-7/data/CUSTOM-ARC-${DATE:0:4}-${DATE:4:2}-${DATE:6:2}-METRIC.csv ]; then
    /home/users/earjham/miniforge3/envs/sonic6/bin/python ${DIR}/../proc_netcdf/process_sonic.py /gws/pw/j07/ncas_obs_vol1/cvao/raw_data/ncas-sonic-6/data/20241127_longterm/${DATE:0:4}/${DATE:4:2}/${DATE}_ncas-sonic-6.csv -o /gws/pw/j07/ncas_obs_vol1/cvao/processing/ncas-sonic-6/20241127_longterm/${DATE:0:4}/${DATE:4:2} -m ${DIR}/../proc_netcdf/metadata.json --aws_7_file /gws/pw/j07/ncas_obs_vol1/cvao/raw_data/ncas-aws-7/data/CUSTOM-ARC-${DATE:0:4}-${DATE:4:2}-${DATE:6:2}-METRIC.csv
else
    /home/users/earjham/miniforge3/envs/sonic6/bin/python ${DIR}/../proc_netcdf/process_sonic.py /gws/pw/j07/ncas_obs_vol1/cvao/raw_data/ncas-sonic-6/data/20241127_longterm/${DATE:0:4}/${DATE:4:2}/${DATE}_ncas-sonic-6.csv -o /gws/pw/j07/ncas_obs_vol1/cvao/processing/ncas-sonic-6/20241127_longterm/${DATE:0:4}/${DATE:4:2} -m ${DIR}/../proc_netcdf/metadata.json
fi
