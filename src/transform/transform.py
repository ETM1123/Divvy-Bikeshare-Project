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

def change_column_name(df : pd.DataFrame, old_name : str, new_name : str) -> pd.DataFrame:
  df = df.rename(columns={old_name:new_name})
  return df

def remove_null_values(df : pd.DataFrame) -> pd.DataFrame:
  df = df.dropna(axis=0, how='any', inplace=False)
  return df 

def run():
  raw_data_path = os.path.join(DATA_DIR, 'raw')
  years = sorted([year for year in os.listdir(raw_data_path) if year.isdigit()])
  for year in years:
    raw_data_year_path = os.path.join(raw_data_path, year)
    filenames : list(str) = [filename for filename in os.listdir(raw_data_year_path) if '.csv' in filename]
    filename = filenames[0]
    filepath = os.path.join(raw_data_year_path, filename)
    df = get_raw_data(filepath)
    clean_df = clean_data(df)
    print(filepath)
    print(df.shape)
    print(clean_df.shape)
    break

if __name__ == '__main__':
  run()

# load data - i.e read data into a pandas df
# check data frame has the correct column names i.e check initial columns 
# change member_status column to membership_status 
# Remove missing entries 