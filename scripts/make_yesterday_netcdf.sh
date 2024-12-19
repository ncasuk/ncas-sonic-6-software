#!/bin/bash

DATE=$(date -d "yesterday" -u +"%Y%m%d")

DIR=$(dirname "$(readlink -f "$0")")

${DIR}/make_netcdf.sh $DATE
