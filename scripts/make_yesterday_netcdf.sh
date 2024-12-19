#!/bin/bash

date=$(date -d "yesterday" -u +"%Y%m%d")

./make_netcdf.sh $date
