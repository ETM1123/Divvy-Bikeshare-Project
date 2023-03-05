import os
from pathlib import Path
from datetime import datetime
import shutil
import pandas as pd
import geopandas as gpd
from etl.extract import download_file_from_web
from etl.transform import (
    add_column, add_geo_field_from_lat_long, combine_data, remove_column,
    select_column, filter_column, sort_data, remove_duplicates, remove_null_values,
    update_column_name, reset_index
)
from dateutil.relativedelta import relativedelta


URL_PREFIX = "https://divvy-tripdata.s3.amazonaws.com"
FILENAME = "divvy-tripdata.zip"

# Util for extract


def extract_divvy_biketrip_dataset(start_date, end_date, destination, date_format: str = "%Y-%m-%d") -> None:
    """Extract the Divvy biketrip dataset for a given date range and save the files
    in a directory.

    Args:
      start_date (str): Start date for the range in the format "YYYY-MM-DD".
      end_date (str): End date for the range in the format "YYYY-MM-DD".
      destination (str): Directory where the extracted files should be saved.
      date_format (str, optional): Format of the date strings. Defaults to "%Y-%m-%d".

    Returns:
      None
    """
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)

    if validate_date(start_date, end_date):
        urls = get_data_urls(start_date, end_date)
        # Download data
        for url in urls:
            filename = url[-25:]
            year = filename[:4]
            path = os.path.join(destination, year)
            create_path(path)
            download_file_from_web(url, filename, path)
    else:
        print("Invalid Date")


def create_path(path: str) -> None:
    """Create directory if it does not exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def format_data_url(date_str: str) -> str:
    """Create the URL to download Divvy biketrip dataset for a given date.
    """
    return f"{URL_PREFIX}/{date_str}-{FILENAME}"


def get_data_urls(start_date, end_date):
    """Generate a list of URLs for downloading Divvy biketrip dataset
    for a range of dates.

    Args:
      start_date (datetime): Start date of the range.
      end_date (datetime): End date of the range.

    Returns:
      List of str: List of URLs for downloading the dataset.
    """
    dates = generate_dates(start_date, end_date)
    filenames = [format_data_url(date) for date in dates]
    return filenames


def generate_dates(start_date: datetime, end_date: datetime) -> list[str]:
    """Generate a list of date strings for a range of dates.

    Args:
      start_date (datetime): Start date of the range.
      end_date (datetime): End date of the range.

    Returns:
      list[str]: List of date strings in the format "YYYYMM".
    """
    dates = []
    current_date = start_date

    while current_date <= end_date:
        dates.append(current_date.strftime("%Y%m"))
        current_date += relativedelta(months=1)

    return dates


def validate_date(start_date, end_date):
    """Validate if the start and end dates for the Divvy biketrip dataset
    download are within a valid range.

    Args:
      start_date: Start date for the range.
      end_date: End date for the range.

    Returns:
      bool: True if dates are within valid range, False otherwise.
    """
    april_2020 = datetime(year=2020, month=4, day=1)  # min date
    current_date = datetime.now()  # max date
    return start_date >= april_2020 and end_date <= current_date


# Util for transform
filepath_state = os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', 'cb_2018_us_state_500k.shp')
filepath_neighborhood = os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', 'chicago_neighborhoods.geojson')
states = gpd.read_file(filepath_state)
neighborhood = gpd.read_file(filepath_neighborhood)


def get_state(lat, lng, row, states=states):
    # print(row[lng], row[lat])
    point = gpd.points_from_xy([row[lng]], [row[lat]])[0]
    result = states[states.contains(point)]

    # If the query returned any results, return the name of the state
    if not result.empty:
        return result.iloc[0]['NAME']
    else:
        return "None"


def transform_data(df) -> pd.DataFrame:

    station_df = build_station_df(df)

    columns = ['start_station_id', 'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng']

    # correct station name to include start and end prefix
    start_station_cols = {col: "start_" + col for col in station_df.columns}
    end_station_cols = {col: "end_" + col for col in station_df.columns}
    start_station_df = station_df.pipe(update_column_name, start_station_cols)
    end_station_df = station_df.pipe(update_column_name, end_station_cols)

    main_df = (df
               .pipe(remove_null_values)
               .pipe(remove_column, columns)
               .pipe(combine_data, start_station_df, 'start_station_name')
               .pipe(combine_data, end_station_df, 'end_station_name')
               .pipe(add_column, 'duration_min', duration_in_minutes, 'started_at', 'ended_at')
               .pipe(filter_column, 'duration_min', 'greater', 0)
               .pipe(filter_column, 'duration_min', 'less', 1440)
               )
    return main_df


def build_station_df(df) -> pd.DataFrame:
    columns = ['start_station_name', 'started_at', 'start_lat', 'start_lng']
    columns_mapping = {'start_station_name': 'station_name',
                       'start_lat': 'lat',
                       'start_lng': 'lng', }
    cols = ['station_id', 'station_name', 'lat', 'lng', 'state', 'pri_neigh', 'sec_neigh']
    c_mapping_2 = {'pri_neigh': 'primary_neighborhood', 'sec_neigh': 'secondary_neighborhood'}

    station_df = (df
                  .pipe(select_column, columns)
                  .pipe(update_column_name, columns_mapping)
                  .pipe(remove_null_values)
                  .pipe(sort_data, 'started_at', 'asc')
                  .pipe(remove_duplicates, "station_name")
                  .pipe(remove_column, "started_at")
                  .pipe(add_column, 'state', get_state, 'lat', 'lng')
                  .pipe(filter_column, 'state', 'equal', 'Illinois')
                  .pipe(reset_index)
                  .pipe(update_column_name, {'index': 'station_id'})
                  .pipe(add_geo_field_from_lat_long, neighborhood, 'lng', 'lat')
                  .pipe(select_column, cols)
                  .pipe(update_column_name, c_mapping_2)
                  )
    return station_df

    # from datetime import datetime, timedelta


def duration_in_minutes(start_time: str, end_time: str, row) -> int:
    """Calculates the duration in minutes '"""

    duration = row[end_time] - row[start_time]
    duration_minutes = duration.total_seconds() // 60
    return int(duration_minutes)

# Testing


def create_files_in_directory(directory: str, file_ext: str, file_content: list[str], num_files: int) -> None:
    """
    Creates files with given file extension in the specified directory.
    Each file created will be named as file0, file1, file2,...fileN.

    Args:
      directory (str): Directory where files will be created.
      file_ext (str): Extension for the files to be created.
      num_files (int): Number of files to be created.
    """
    create_path(directory)
    if len(file_content) == num_files:
        for i in range(num_files):
            filename = os.path.join(directory, f"file{i}{file_ext}")
            with open(filename, 'w') as f:
                f.write(file_content[i])


def clear_directory(dir_path: str) -> None:
    """Removes all files and directories in the provided directory. This will
    permanently delete All content in provide directory - use with cation
    """

    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")


def add_5(col: str, row: pd.Series) -> int:
    return row[col] + 5   # type: ignore


if __name__ == "__main__":
    # print(test_data_directory)
    filename = 'combined_biketrip_data.csv'
    filepath: str = os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', filename)
    data_types = {
        "ride_id": str,
        "rideable_type": str,
        "start_station_name": str,
        "end_station_name": str,
        "start_station_id": str,
        "end_station_id": str,
        "start_lat": float,
        "start_lng": float,
        "end_lat": float,
        "end_lng": float,
        "member_casual": str,
    }

    def date_parser(date): return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    parse_dates: list[str] = ['started_at', 'ended_at']
    df = pd.read_csv(filepath, dtype=data_types, date_parser=date_parser, parse_dates=parse_dates, nrows=1000)
    df = transform_data(df)
    print(df.head())
