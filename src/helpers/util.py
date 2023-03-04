import os
import zipfile
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Callable, List, Tuple, Union
import pandas as pd
import geopandas as gpd
from transform.transform import *


filepath_state =  os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', 'cb_2018_us_state_500k.shp')
filepath_neighborhood =  os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', 'chicago_neighborhoods.geojson')
states = gpd.read_file(filepath_state)
neighborhood = gpd.read_file(filepath_neighborhood)



def get_state(lat, lng, row, states = states):
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
  start_station_cols ={ col: "start_"+ col for col in station_df.columns}
  end_station_cols ={ col: "end_"+ col for col in station_df.columns}
  start_station_df = station_df.pipe(update_column_name, start_station_cols )
  end_station_df = station_df.pipe(update_column_name, end_station_cols)

  main_df = (df
             .pipe(remove_column, columns)
             .pipe(combine_data, start_station_df, 'start_station_name')
             .pipe(combine_data, end_station_df, 'end_station_name')
             )
  return main_df

def build_station_df(df) -> pd.DataFrame:
  columns = ['start_station_name', 'started_at', 'start_lat', 'start_lng']
  columns_mapping={'start_station_name' : 'station_name',
           'start_lat' : 'lat',
           'start_lng' : 'lng',}
  cols = ['station_id','station_name', 'lat', 'lng','state', 'pri_neigh', 'sec_neigh']
  c_mapping_2 = {'pri_neigh':'primary_neighborhood','sec_neigh': 'secondary_neighborhood'}

  station_df = (df
                .pipe(select_column, columns)
                .pipe(update_column_name, columns_mapping)
                .pipe(remove_null_values)
                .pipe(sort_data,'started_at', 'asc')
                .pipe(remove_duplicates, "station_name")
                .pipe(remove_column, "started_at")
                .pipe(add_column, 'state', get_state, 'lat', 'lng')
                .pipe(filter_column, 'state','equal', 'Illinois')
                .pipe(reset_index)
                .pipe(update_column_name, {'index':'station_id'})
                .pipe(add_geo_field_from_lat_long, neighborhood, 'lng', 'lat')
                .pipe(select_column, cols)
                .pipe(update_column_name, c_mapping_2)
         )
  return station_df



if __name__ == "__main__":
  # print(test_data_directory)
  filename = 'combined_biketrip_data.csv'
  filepath : str = os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', filename)
  data_types = {
  "ride_id" : str,
  "rideable_type" : str,
  "start_station_name" : str,
  "end_station_name" : str,
  "start_station_id" : str,
  "end_station_id" : str,
  "start_lat" : float,
    "start_lng" : float,
    "end_lat" : float,
    "end_lng" : float,
    "member_casual" : str,
  }

  date_parser  = lambda date: datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
  parse_dates : list[str] = ['started_at', 'ended_at']
  df = pd.read_csv(filepath, dtype=data_types, date_parser=date_parser, parse_dates=parse_dates, nrows=1000)
  df = transform_data(df)
  print(df.head())