# Cleans process the data 

import datetime
import os
from pathlib import Path
import pandas as pd

DATA_DIR : str = os.path.join(str(Path(__file__).parents[2]), 'data')


def get_raw_data(filename : str) -> pd.DataFrame:
  date_parser  = lambda date: datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
  parse_dates : list[str] = ['started_at', 'ended_at']
  # filepath = os.path.join(DATA_DIR, str(year), filename)
  data : pd.DataFrame = pd.read_csv(filename,
                                 parse_dates = parse_dates,
                                 date_parser = date_parser)
  return data


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
  clean_df = (df.pipe(remove_null_values)
                .pipe(cast_values_to_str, 'start_station_id', 'end_station_id')
                .pipe(clean_station_id, 'start_station_id')
                .pipe(clean_station_id, 'end_station_id')
                )

  return clean_df

def remove_period(s: str) -> str:
  s = s.strip()
  if '.0' in s:
    return s[:-2]
  return s

def clean_station_id(df : pd.DataFrame, colname: str) -> pd.DataFrame:
  df[colname] = df[colname].apply(lambda station_id: remove_period(station_id))
  return df

def cast_values_to_str(df, *colnames) -> pd.DataFrame:
  new_col_types = {colname: str for colname in colnames}
  df = df.astype(new_col_types)
  return df

def change_column_name(df : pd.DataFrame, **col_name_mapping : dict[str, str]) -> pd.DataFrame:
  df = df.rename(columns=col_name_mapping)
  return df

def remove_null_values(df : pd.DataFrame) -> pd.DataFrame:
  df = df.dropna(axis=0, how='any', inplace=False)
  return df 

def get_station_info(df : pd.DataFrame) -> pd.DataFrame:
  start_station_cols = ['start_station_id', 'start_station_name', 'start_lat', 'start_lng']
  end_station_cols = ['end' + colname[5:] for colname in start_station_cols]
  new_col_names = ['station_id', 'station_name', 'latitude', 'longitude']
  new_start_station_cols = {old_name: new_name for old_name, new_name in zip(start_station_cols, new_col_names)}
  new_end_station_cols = {old_name: new_name for old_name, new_name in zip(end_station_cols, new_col_names)}


  start_station_info = df.loc[:, start_station_cols].pipe(change_column_name, **new_start_station_cols)
  end_station_info = df.loc[:, end_station_cols].pipe(change_column_name, **new_end_station_cols)

  stations = (pd.concat([start_station_info, end_station_info])
              .drop_duplicates(subset=['station_id', 'station_name'], keep='last')
              .reset_index(drop=True))
  return stations


def create_files(master_station_filename : str, 
                 station_mapping_filename : str, 
                 biketrip_filename : str, 
                 original_station_info : pd.DataFrame, 
                 rides : pd.DataFrame) -> None:
  """Creates three csv fils that tracks:
      1. Original stations_id mapping with their corresponding information
      2. New station_id mapping (that we generate) for unique station name
      3. Unique bike trips with the new station_id mappings
  """
  stations = (original_station_info
                  .loc[:, ['station_name', 'latitude', 'longitude']]
                  .drop_duplicates(subset=['station_name'], keep='last')
                  .reset_index(drop=True))
  # Add new station_id mapping 
  stations.insert(0, 'ID', [i+1 for i in range(stations.shape[0])])

  # update rides_df with new station_id mapping 
  master_rides_df = update_station_ids(rides, stations)

  # save file in process directory
  original_station_info.to_csv(master_station_filename, index=False)
  stations.to_csv(station_mapping_filename, index=False)
  master_rides_df.to_csv(biketrip_filename, index=False)

def update_station_ids(rides, stations):
  """Map the station names from rides df to their corresponding station_ids from stations df"""
  df1 = (pd.merge(rides, stations, left_on='start_station_name', right_on='station_name')
         .loc[:, ['ride_id', 'rideable_type', 'ID', 'started_at', 'ended_at', 'end_station_name']]
         .rename(columns={'ID':'from_station_id'}))
  
  df2 = (pd.merge(df1, stations, left_on='end_station_name', right_on='station_name')
         .loc[:, ['ride_id', 'rideable_type', 'from_station_id','ID', 'started_at', 'ended_at']]
         .rename(columns={'ID':'to_station_id'})
         .reset_index(drop=True))

  return df2

def get_rides_info(df : pd.DataFrame) -> pd.DataFrame:
  rides_col : str = ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'end_station_name']
  return df.loc[:, rides_col]

def run():
  raw_data_path = os.path.join(DATA_DIR, 'raw')
  years = sorted([year for year in os.listdir(raw_data_path) if year.isdigit()])
  for year in years:
    raw_data_year_path = os.path.join(raw_data_path, year)
    filenames : list(str) = [filename for filename in os.listdir(raw_data_year_path) if '.csv' in filename]
    filename = filenames[0]
    filepath = os.path.join(raw_data_year_path, filename)
    df = get_raw_data(filepath)
    return df
    # clean_df = clean_data(df)
    # stations = get_station_info(clean_df)
    # rides = get_rides_info(clean_df)
    # create_files('stations.csv',
    #              'mapping.csv',
    #              'rides.csv',
    #              stations,
    #              rides)
    # x = pd.read_csv('stations.csv')
    # y = pd.read_csv('mapping.csv')
    # z = pd.read_csv('rides.csv')
    # print(x.shape)
    # print(y.shape)
    # print(z.shape)
    # break

# if __name__ == '__main__':
  # run()

# load data - i.e read data into a pandas df
# check data frame has the correct column names i.e check initial columns 
# change member_status column to membership_status 
# Remove missing entries 