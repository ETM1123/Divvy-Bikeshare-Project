from datetime import datetime
import os
from pathlib import Path
import pandas as pd


def date_parser(date): return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')


parse_dates: list[str] = ['started_at', 'ended_at']
filepath = os.path.join(str(Path(__file__).parents[2]), "data", 'final', 'divvy_final.csv')
default_cols = [
    'ride_id',
    'rideable_type',
    'started_at',
    'ended_at',
    'start_station_name',
    'end_station_name',
    'member_casual',
    'start_station_id',
    'start_lat',
    'start_lng',
    'start_primary_neighborhood',
    'start_secondary_neighborhood',
    'end_station_id',
    'end_lat',
    'end_lng',
    'end_primary_neighborhood',
    'end_secondary_neighborhood',
    'duration_min'
]


def load_data(row_num: int, default_cols: list[str] = default_cols) -> pd.DataFrame:
    return pd.read_csv(filepath, nrows=row_num, usecols=default_cols, date_parser=date_parser, parse_dates=parse_dates)
