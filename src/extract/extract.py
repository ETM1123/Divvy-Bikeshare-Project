from time import time 
from datetime import datetime
import subprocess
import os
from dateutil.relativedelta import relativedelta
import pandas as pd
import glob

# "https://divvy-tripdata.s3.amazonaws.com/202004-divvy-tripdata.zip"
URL_PREFIX = "https://divvy-tripdata.s3.amazonaws.com"
FILENAME = "divvy-tripdata.zip"


def create_path(path: str) -> None:
  if not os.path.exists(path):
    os.makedirs(path)

def format_data_url(date_str : str) -> str:
  return f"{URL_PREFIX}/{date_str}-{FILENAME}" 

def get_data_urls(start_date, end_date):
  dates = generate_dates(start_date, end_date)
  filenames = [format_data_url(date) for date in dates]
  return filenames

def generate_dates(start_date : datetime, end_date : datetime) -> list[str]:
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
      dates.append(current_date.strftime("%Y%m"))
      current_date += relativedelta(months=1)
    
    return dates

def validate_date(start_date, end_date):
  april_2020 = datetime(year=2020, month=4, day=1) # min date
  current_date = datetime.now() # max date 
  return start_date >= april_2020 and end_date <= current_date

def download_file_from_web(url: str, filename: str, destination: str, compressed=True) -> None:
  try:
    data_path = os.path.join(destination, filename)
    # Download data
    subprocess.run(f'curl -sSL {url} > {data_path}', shell=True, check=True)
    if compressed:
      # Extract zipfile content
      subprocess.run(f'unzip -o {data_path} -d {destination}', shell=True, check=True)
      # Remove zipfile
      subprocess.run(f'rm {data_path}', shell=True, check=True)
  except subprocess.CalledProcessError as e:
    print(f"Error: {e}")

def extract_divvy_biketrip_dataset(start_date, end_date, destination, date_format : str = "%Y-%m-%d") -> None:
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


def combine_csv_files(input_dir: str, output_file: str) -> None:
 
  start_year, end_year = 2020, 2022
  df_list = []

  for year in range(start_year, end_year + 1):
    data_dir = os.path.join(input_dir, str(year))
    all_files = glob.glob(data_dir + "/*.csv")
    for filename in all_files:
      df = pd.read_csv(filename, index_col=None, header=0)
      table_name = filename.replace(".csv", "")
      df['table_name'] = table_name # keep track of the file
      df_list.append(df)
      print(f"added {filename} to df list")

  combined_df = pd.concat(df_list, axis=0, ignore_index=True)
   # Write the combined data to a single CSV file
  combined_df.to_csv(output_file, index=False)