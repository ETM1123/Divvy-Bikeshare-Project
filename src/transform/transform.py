# Cleans process the data 

import datetime
import os
from pathlib import Path
from typing import Tuple
import pandas as pd
import geopandas as gpd

DATA_DIR : str = os.path.join(str(Path(__file__).parents[2]), 'data')

calliable = None

def transfrom_data(df) -> pd.DataFrame:

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
                .pipe(remove_duplicates, "station_name", 'asc')
                .pipe(remove_column, "started_at")
                .pipe(add_column, 'state', calliable)
                .pipe(filter_data, 'state', condition = True)
                .pipe(add_column, 'station_id', callable)
                .pipe(add_geo_field, None, 'lng', 'lat')
                .pipe(select_column, cols)
                .pipe(update_column_name, c_mapping_2)
         )
  return station_df

def add_geo_field(df, geo_df, x, y, ) -> pd.DataFrame:
  # create a GeoSeries of Point objects and convert to geopandas
  geometry = gpd.points_from_xy(df[x], df[y])
  df_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
  # Set up correct projection
  df_gdf = df_gdf.set_crs(epsg=4326)
  geo_df = geo_df.set_crs(epsg=4326)
  # Join tables
  combined_gdf = df_gdf.sjoin(geo_df, how="inner", predicate='within').reset_index(drop=True)
  return pd.DataFrame(combined_gdf)

def combine_data(df1, df2) -> pd.DataFrame:
  return

def remove_column(df, cols) -> pd.DataFrame:
  return 

def select_column(df, cols) -> pd.DataFrame:
  return

def filter_data(df, condition) -> pd.DataFrame:
  return 

def sort_data(df, by, order) -> pd.DataFrame:
  return 

def remove_duplicates(df, method) -> pd.DataFrame:
  return 

def remove_null_values(df : pd.DataFrame) -> pd.DataFrame:
  df = df.dropna(axis=0, how='any', inplace=False)
  return df

def update_column_name(df : pd.DataFrame, **col_name_mapping : dict[str, str]) -> pd.DataFrame:
  df = df.rename(columns=col_name_mapping)
  return df

def add_column(df, col_name, calliable) -> pd.DataFrame:
  return
  
   