"""
IMPORTANT NOTE ABOUT GILL SONIC
u and v winds are reported wrong...

Meteorology:
positive u wind is west to east
positive v wind is south to north

Gill:
positive u wind is south to north
positive v wind is east to west

Convert Gill to meteorology
met_u = -gill_v
met_v = gill_u
"""

import polars as pl
import numpy as np
import ncas_amof_netcdf_template as nant
import datetime as dt


def find_closest_time_match(df_small: pl.DataFrame, df_large: pl.DataFrame) -> pl.DataFrame:
    if "time" not in df_small.columns or not all(isinstance(x, dt.datetime) for x in df_small["time"]):
        raise ValueError("df_small must have a 'time' column of datetime objects.")
    if "time" not in df_large.columns or not all(isinstance(x, dt.datetime) for x in df_large["time"]):
        raise ValueError("df_large must have a 'time' column of datetime objects.")
    if df_small.is_empty():
        return pl.DataFrame()  # Return empty DataFrame if df_small is empty
    df_combined = []  # List to hold the combined rows
    for row in df_small.iter_rows(named=True):  # Iterate through rows of df_small
        target_time = row["time"]
        closest_match = None
        min_diff = dt.timedelta.max
        for large_row in df_large.iter_rows(named=True): # Iterate through rows of df_large
            diff = abs(target_time - large_row["time"])
            if diff < min_diff:
                min_diff = diff
                closest_match = large_row
            else:
                break
        if closest_match:
            combined_row = {**row, **{k: v for k, v in closest_match.items() if k != "time"}}
            df_combined.append(combined_row)
    return pl.DataFrame(df_combined)


def check_wind_dir_consistency(df, aws_7_file, diff=45):
    # read data and convert times to datetime
    if aws_7_file is not None:
        aws_7 = pl.read_csv(aws_7_file, columns=["Timestamp (UTC)", "Winddir / °"], new_columns=["time", "winddir"], null_values="NULL")
        aws_7 = aws_7.with_columns(pl.col("time").str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC"))
    else:
        raise ValueError("No data files provided for consistency check")

    # check wind direction consistency
    df = find_closest_time_match(df, aws_7)
    new_status = []
    for i in range(len(df)):
        if df["status"][i] in ["00", 0] and df["winddir"][i] != "NULL":
            if 360-diff > abs(df["wind_from_direction"][i] - float(df["winddir"][i])) > diff:
                new_status.append(8)
            else:
                new_status.append(df["status"][i])
        else:
            new_status.append(df["status"][i])
    df = df.with_columns([
        pl.Series(new_status).alias("status"),
    ])

    return df


def main(infile, outdir="./", metadata_file="metadata.json", aws_7_file=None):
    # read data
    df = pl.read_csv(infile, has_header=False, new_columns=["time", "node", "gill_u", "gill_v", "units", "status", "check"])
    
    # convert wind speed units if needed
    if (df["units"] != "M").any():
        new_gill_u = []
        new_gill_v = []
        for i, unit in enumerate(df["units"]):
            if unit == "N":  # knots
                new_gill_u = df["gill_u"][i]*0.51444
                new_gill_v = df["gill_v"][i]*0.51444
            elif unit == "P":  # miles per hour
                new_gill_u = df["gill_u"][i]*0.44704
                new_gill_v = df["gill_v"][i]*0.44704
            elif unit == "K":  # kilometres per hour
                new_gill_u = df["gill_u"][i]*0.27777
                new_gill_v = df["gill_v"][i]*0.27777
            elif unit == "F":  # feet per minute
                new_gill_u = df["gill_u"][i]*0.00508
                new_gill_v = df["gill_v"][i]*0.00508
            elif unit != "M":  # metres per second
                msg = f"Unknown unit found: {unit}"
                raise ValueError(msg)
        df = df.with_columns([
            pl.Series("gill_u", new_gill_u),
            pl.Series("gill_v", new_gill_v),
        ])
    
    # Gill reports u and v winds wrong...
    # It reports "positive u wind" as wind from south to north, and
    # "positive v wind" as wind from east to west
    # convert to meteorology winds
    df = df.with_columns([
        (-df["gill_v"]).alias("met_u"),
        (df["gill_u"]).alias("met_v"),
    ])
    
    # convert times to datetime
    df = df.with_columns(pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", time_zone="UTC"))
    
    # Get all the time formats
    unix_times, day_of_year, years, months, days, hours, minutes, seconds, time_coverage_start_unix, time_coverage_end_unix, file_date = nant.util.get_times(df["time"])

    # Create netCDF file
    nc = nant.create_netcdf.main("ncas-sonic-6", date=file_date, dimension_lengths={"time": len(unix_times)}, products="mean-winds", file_location=outdir, product_version="1.1")
    if isinstance(nc, list):
        print("[WARNING] Unexpectedly got multiple netCDFs returned from nant.create_netcdf.main, just using first file...")
        nc = nc[0]
    
    # Add time variable data to netCDF file
    nant.util.update_variable(nc, "time", unix_times)
    nant.util.update_variable(nc, "day_of_year", day_of_year)
    nant.util.update_variable(nc, "year", years)
    nant.util.update_variable(nc, "month", months)
    nant.util.update_variable(nc, "day", days)
    nant.util.update_variable(nc, "hour", hours)
    nant.util.update_variable(nc, "minute", minutes)
    nant.util.update_variable(nc, "second", seconds)
    
    # Add wind data from sonic to netCDF file
    nant.util.update_variable(nc, "eastward_wind", df["met_u"])
    nant.util.update_variable(nc, "northward_wind", df["met_v"])
    
    # Add computed wind data to netCDF file
    wind_speed = pl.Series((df["met_u"] ** 2 + df["met_v"] ** 2) ** 0.5).round(2)
    nant.util.update_variable(nc, "wind_speed", wind_speed)
    # add wind_speed to df
    df = df.with_columns([
        wind_speed.alias("wind_speed"),
    ])
    
    wind_from_direction = pl.Series((270 - np.arctan2(df["met_v"], df["met_u"]) * 180 / np.pi) % 360)
    nant.util.update_variable(nc, "wind_from_direction", wind_from_direction)
    # add wind_direction to df
    df = df.with_columns([
        wind_from_direction.alias("wind_from_direction"),
    ])

    # check wind direction consistency with other aws data
    if aws_7_file is not None:
        df = check_wind_dir_consistency(df, aws_7_file)
    
    # Check status codes to create qc flag data
    qc_vals = []
    qc_meanings = ["not_used", "good_data", "insufficient_samples_in_average_period_along_u_axis", "insufficient_samples_in_average_period_along_v_axis", "insufficient_samples_in_average_period_along_both_axis", "wind_direction_inconsistent_with_other_weather_stations"]
    
    for s in df["status"]:
        if s in ["00", 0]:
            # all good
            qc_vals.append(1)
        elif s in ["01", 1]:
            # gill u axis = met v axis
            qc_vals.append(3)
        elif s in ["02", 2]:
            # gill v axis = met u axis
            qc_vals.append(2)
        elif s in ["04", 4]:
            # both axis
            qc_vals.append(4)
        elif s in ["08", 8]:
            # inconsistent with other weather stations
            qc_vals.append(5)
        else:
            print(s)
            raise ValueError("Unknown status code")
    
    # Update qc meanings and add qc data
    for varname in ["qc_flag_wind_component_eastward", "qc_flag_wind_component_northward", "qc_flag_wind_speed", "qc_flag_wind_direction"]:
        nant.util.change_qc_flags(nc, varname, flag_meanings=qc_meanings)
    nant.util.update_variable(nc, "qc_flag_wind_component_eastward", qc_vals)
    nant.util.update_variable(nc, "qc_flag_wind_component_northward", qc_vals)
    nant.util.update_variable(nc, "qc_flag_wind_speed", qc_vals)
    nant.util.update_variable(nc, "qc_flag_wind_direction", qc_vals)
    
    # Add time_coverage_start and time_coverage_end metadata using data from get_times
    nc.setncattr(
        "time_coverage_start",
        dt.datetime.fromtimestamp(time_coverage_start_unix, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    )
    nc.setncattr(
        "time_coverage_end",
        dt.datetime.fromtimestamp(time_coverage_end_unix, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    )

    # Add metadata from file
    nant.util.add_metadata_to_netcdf(nc, metadata_file)

    # Close file, remove empty
    file_name = nc.filepath()
    nc.close()
    nant.remove_empty_variables.main(file_name)


def none_or_str(value):
    if value == 'None':
        return None
    return value


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process Gill Sonic data to netCDF")
    parser.add_argument("infile", type=str, help="Input file")
    parser.add_argument("-o", "--outdir", type=str, default="./", help="Output directory")
    parser.add_argument("-m", "--metadata_file", type=str, default="metadata.json", help="Metadata file")
    parser.add_argument("--aws_7_file", type=none_or_str, default=None, help="Data file from ncas-aws-7 for consistency check")
    args = parser.parse_args()
    main(args.infile, outdir=args.outdir, metadata_file=args.metadata_file, aws_7_file=args.aws_7_file)
