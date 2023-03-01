from time import time 
from datetime import datetime
import subprocess
import os
from dateutil.relativedelta import relativedelta

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

def download_zipfile(url: str, filename: str, destination: str) -> None:
  try:
    data_path = os.path.join(destination, filename)
    # Download data
    subprocess.run(f'curl -sSL {url} > {data_path}', shell=True, check=True)
    # Extract zipfile content
    subprocess.run(f'unzip -o {data_path} -d {destination}', shell=True, check=True)
    # Remove zipfile
    subprocess.run(f'rm {data_path}', shell=True, check=True)
  except subprocess.CalledProcessError as e:
    print(f"Error: {e}")

  return None

def clean_up_zipfiles(path):
  try:
    # Delete zipfile
    subprocess.run(f'rm {path}/*.zip', shell=True, check=True)
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
      download_zipfile(url, filename, path)
  else:
    print("Invalid Date")
