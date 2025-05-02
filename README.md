# ncas-sonic-6-software

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15322281.svg)](https://doi.org/10.5281/zenodo.15322281)


Processing code for converting data from the Gill 2D Sonic Anemometer into netCDF files conforming to the NCAS General Standard.

## Scripts

Three bash scripts are provided in the scripts folder:
* `setup.sh` - this creates the python environment needed
* `make_netcdf.sh` - makes netCDF file for given date provided the raw data file exists
* `make_yesterday_netcdf.sh` - passes yesterday's date to `make_netcdf.sh`
