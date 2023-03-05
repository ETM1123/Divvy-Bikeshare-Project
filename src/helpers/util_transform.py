from pathlib import Path
from etl.transform import (
    add_column, add_geo_field_from_lat_long, combine_data, remove_column,
    select_column, filter_column, sort_data, remove_duplicates, remove_null_values,
    update_column_name, reset_index
)
import geopandas as gpd
import pandas as pd
import os

filepath_state = os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', 'cb_2018_us_state_500k.shp')
filepath_neighborhood = os.path.join(str(Path(__file__).parents[2]), 'data', 'processed', 'chicago_neighborhoods.geojson')
states = gpd.read_file(filepath_state)
neighborhood = gpd.read_file(filepath_neighborhood)


def get_state(lat, lng, states, row):
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
                  .pipe(add_column, 'state', get_state, 'lat', 'lng', states)
                  .pipe(filter_column, 'state', 'equal', 'Illinois')
                  .pipe(reset_index)
                  .pipe(update_column_name, {'index': 'station_id'})
                  .pipe(add_geo_field_from_lat_long, neighborhood, 'lng', 'lat')
                  .pipe(select_column, cols)
                  .pipe(update_column_name, c_mapping_2)
                  )
    return station_df


def duration_in_minutes(start_time: str, end_time: str, row) -> int:
    """Calculates the duration in minutes '"""
    duration = row[end_time] - row[start_time]
    duration_minutes = duration.total_seconds() // 60
    return int(duration_minutes)
