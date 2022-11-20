""" 
In this file we - we are going to create helper functions that will help us scrape data from the webpage.
"""

# imports 
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from typing import List
from time import sleep
from datetime import datetime
import pandas as pd
import urllib.request
import zipfile
import os
import shutil

# Idea 
# Extract information from the URL
# The webpage contains a table with the following content:
# Downloadable zipfile link, the last date  content of the zipfile was modified, and the size of the zip file

# We are not interested in all of the zip files on the webpage - we are only interested 
# in the zip files with the following name conventions 202004-divvy-tripdata.zip 
# i.e year+month-company name 

# We want to extract all of the information 

def extract_zipfile_metadata(url: str = "", destination: str = "", metadata_filename : str = "zipfile_metadata.csv" ) -> None:
  """Extracts and saves the name of the zip file, the last date it was modified, and the file size from the 
  table located on the url page. Saves the extracted content as a csv file in destination path.

  args:
    url (str): link to the webpage where the content is stored in html and java script format.
    destination (str): Path to the location where to store data.
  """
  webpage_content : str = get_webpage_content(url)
  zipfile_metadata : dict = webpage_content_to_dict(webpage_content)
  files_in_directory : List[str] = [file for file in os.listdir(destination) if file[-4:] == ".csv"]

  if metadata_filename in files_in_directory:
    update_metadata(zipfile_metadata, destination, metadata_filename)

  else:
    save_metadata(zipfile_metadata, destination, metadata_filename)


def get_webpage_content(url: str) -> str:
  options = ChromeOptions()
  options.headless = True
  driver = Chrome(options=options)
  driver.get(url)

  table_id = "tbody-content"
  data = driver.find_element(By.ID, table_id)
  sleep(3)
  return data.text

def webpage_content_to_dict(webpage_content: str) -> dict:
  data : dict = {"filename": [], "last_modified_date": [], "filesize": []}
  table_content : List[str] = webpage_content.split(" ")

  for row in table_content:
    if row[:6].isdigit():
      filename, last_modified_date, filesize = extract_row_content(row)
      data["filename"].append(filename)
      data["last_modified_date"].append(last_modified_date)
      data["filesize"].append(filesize)

  return data 

def save_metadata(data : dict, destination: str, filename : str) -> None:
  filepath : str = f"{destination}/{filename}"
  pd.DataFrame.from_dict(data).to_csv(filepath, index= False)

def update_metadata(data : dict, destination : str, filename: str) -> None:
  filepath : str = f"{destination}/{filename}"
  filenames, modified_dates, filesize = list(data.values())
  rows = zip(filenames, modified_dates, filesize)

  zipfile_metadata : pd.DataFrame = pd.read_csv(filepath, parse_date = ["last_modified_date"])

  for row in rows:
    name, date, size = row
    if zipfile_metadata.loc[zipfile_metadata["filename"] == name].shape[0] > 0:
      logged_date : datetime = pd.Timestamp(zipfile_metadata.loc[zipfile_metadata["filename"] == name, "last_modified_date"].values[0]).to_pydatetime()
      if date > logged_date:
        zipfile_metadata.loc[zipfile_metadata["filename"] == name] = [name, date, size]

    else:
      zipfile_metadata.loc[len(zipfile_metadata)] = [name, date, size]

  save_metadata(zipfile_metadata.to_dict(orient="list"), destination, filename)


def extract_row_content(row : str) -> tuple[str, datetime, str]:
  row_content : List[str] = row.split(" ")

  month = row_content[1]
  day = row_content[2][:-2] # Strip the last 2 elements i.e 2nd, 5th, 18th
  year = row_content[3][:-1] # Remove comma at end of text
  # Extract time information
  hour_minute = ":".join(row_content[4].split(":")[:-1])
  pm_am = row_content[5].upper()
  time = "".join([hour_minute, pm_am])
  # Combine date time information as a dt object
  last_modified_date = datetime.strptime(" ".join([month, day, year, time]), "%b %d %Y %I:%M%p")
  # File information
  filename = row_content[0]
  file_size = " ".join(row_content[6:8])

  return filename, last_modified_date, file_size

def archive_data(destination : str, filename: str) -> None:
  year : str = filename[:5]
  from_path : str = f"{destination}/{year}/{filename}"
  archive_path : str = f"{destination}/archive"
  files_in_archive : List[str] = [file for file in os.listdir(archive_path) if file[:-4] == ".csv"]
  if filename in files_in_archive:
    j = len(filename[:-4]) # extracts name part of the filename
    version_num = len([f for f in files_in_archive if filename[:j] == f[:j]])
    new_filename = f"{filename[:j]}_{version_num + 1}.csv"
    to_path = f"{archive_data}/{new_filename}"
    shutil.make_move(from_path, to_path)
  
  else:
    to_path : str = f"{archive_path}/{filename}"
    shutil.move(from_path, to_path)

def extract_zipfile(url: str, destination: str = "", metadata_filename : str = "") -> None:
  """Downloads and unzips the zip file content to destination

  Args:
      destination (str): Path to the location where to store data. 
  """
  extract_zipfile_metadata() 
  filepath : str = f"{destination}/{metadata_filename}"
  metadata : pd.DataFrame = pd.read_csv(filepath, parse_dates=["last_modified_date"])

  zipfile_filenames = list(metadata.filename)
  source_and_filename = [(f"{url}/{filename}", filename) for filename in zipfile_filenames]

  for source, filename in source_and_filename:
    file, _ = urllib.request.urlretrieve(source)
    with zipfile.ZipFile(file) as zipfile_content:
      year : str = filename[:5]
      path : str = f"{destination}/{year}"
      if filename not in [f for f in os.listdir(path)]:
        zipfile_content.extractall(path)





