# Cleans process the data 

import datetime
import os
from pathlib import Path
from typing import Tuple
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


def update_station_mapping(master_station_filename : str, station_mapping_filename : str, stations : pd.DataFrame) -> None:
  """Update the content of master_station_filename and station_filename if the 
  station_df contains stations that is not in the current files"""
  col_types = {'station_id' : str, 'station_name' : str, 'latitude' : float, 'longitude': float}
  master_station_df = pd.read_csv(master_station_filename, dtype=col_types)
  difference = get_difference_of_station_ids(stations, master_station_df)
  if len(difference) > 0:
    new_station_df = stations.loc[stations['station_id'].isin(difference)]
    station_mapping_df = pd.read_csv(station_mapping_filename)
    updated_master_station, updated_station_mapping = add_station_ids(new_station_df, master_station_df, station_mapping_df)
    save_file(updated_master_station, master_station_df, master_station_filename)
    save_file(updated_station_mapping, station_mapping_df, station_mapping_filename)

def update_biketrip(rides : pd.DataFrame, station_mapping_filename : str, biketrip_filename : str) -> None:
  stations = pd.read_csv(station_mapping_filename)
  master_rides = pd.read_csv(biketrip_filename)
  new_rides = update_station_ids(rides, stations)
  updated_rides = pd.concat([master_rides, new_rides]).reset_index(drop=True)
  save_file(updated_rides, master_rides, biketrip_filename)


def get_difference_of_station_ids(stations, master_station_df):
  """Returns the station_ids that in stations df but not in master_station_df"""
  station_ids = set(stations['station_id'])
  master_station_ids = set(master_station_df['station_id'])
  diff = station_ids.difference(master_station_ids)
  return diff

def add_station_ids(new_stations, master_stations, station_mapping : pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
  """Returns two pandas data frames:
    1. Combine new_station df and master_station df together.
    2. Add the new station names to station_mapping df.
  """
  master_stations = pd.concat([master_stations, new_stations]).reset_index(drop=True)
  new_station_names = set(new_stations['station_name'])
  current_station_name = station_mapping['station_name'].values

  for name in new_station_names:
    if name not in current_station_name:
      cols = ['station_name', 'latitude', 'longitude']
      new_station_dict = new_stations.loc[new_stations['station_name'] == name, cols].to_dict('records')[0]
      # add new id for station name
      new_station_dict['ID'] = len(station_mapping) + 1
      station_mapping = station_mapping.append(new_station_dict, ignore_index=True)

  return master_stations, station_mapping

def save_file(updated_df : pd.DataFrame, original_df : pd.DataFrame, filename : str) -> None:
  """Save the updated_df if content does not match original df """
  not_equal = ~updated_df.equals(original_df)
  if not_equal:
    updated_df.to_csv(filename, index=False)

  
def get_rides_info(df : pd.DataFrame) -> pd.DataFrame:
  rides_col : str = ['ride_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_name', 'end_station_name']
  return df.loc[:, rides_col]

def run(filename,
        path,
        master_station_filename,
        station_mapping_filename,
        biketrip_filename):
  df = get_raw_data(filename)
  df = clean_data(df)
  # extract station id info
  stations = get_station_info(df)
  # extract biketrip info
  rides = get_rides_info(df)
  # filenames
  processed_files = [file for file in os.listdir(path) if '.csv' in file]

  if master_station_filename in processed_files:
    update_station_mapping(master_station_filename, station_mapping_filename, stations)
    update_biketrip(rides, station_mapping_filename, biketrip_filename)
  else:
    create_files(master_station_filename, station_mapping_filename, biketrip_filename, stations, rides)

if __name__ == '__main__':
  # path = '.'
  processed_data_path = os.path.join(DATA_DIR, 'processed')
  master_station_filename = os.path.join(processed_data_path,'original_station_mapping.csv')
  station_mapping_filename = os.path.join(processed_data_path,'new_station_mapping.csv',)
  biketrip_filename = os.path.join(processed_data_path,'rides.csv')
  raw_data_path = os.path.join(DATA_DIR, 'raw', '2020')
  filenames = sorted([file for file in os.listdir(raw_data_path)])
  filename = os.path.join(raw_data_path, filenames[0])
  # print(filenames)
  run(filename,
      processed_data_path,
      master_station_filename,
      station_mapping_filename,
      biketrip_filename)

# load data - i.e read data into a pandas df
# check data frame has the correct column names i.e check initial columns 
# change member_status column to membership_status 
# Remove missing entries 