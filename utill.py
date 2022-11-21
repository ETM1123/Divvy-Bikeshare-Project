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
from pathlib import Path



def extract_zipfile_metadata(data_directory: str, metadata_filename : str, url: str = "https://divvy-tripdata.s3.amazonaws.com/index.html") -> None:
  """Extracts and saves the metadata from url i.e (filename, last modified date, file size) as csv file if the metadata_filename does not exist 
  in the data_directory. If the metadata_filename exists in the data directory, then update the metadata file if new contents are added or modified.

  Args:
      data_directory (str): A path where all of the data are located
      metadata_filename (str): Filename for the 
      url (str, optional): URL to the webpage that contains the metadata about the zip files. Defaults to "https://divvy-tripdata.s3.amazonaws.com/index.html".
  """
  webpage_content : str = get_webpage_content(url)
  zipfile_metadata : dict = webpage_content_to_dict(webpage_content)
  path : str = get_path(data_directory)
  files_in_directory : List[str] = [file for file in os.listdir(path) if file[-4:] == ".csv"]

  if metadata_filename in files_in_directory:
    update_metadata(zipfile_metadata, data_directory, metadata_filename)

  else:
    save_metadata(zipfile_metadata, data_directory, metadata_filename)


def get_webpage_content(url: str) -> str:
  """Opens the url page and extracts the desired data (i.e a table containing information about different downloadable zip files on Divvy bike trip data) as a string

  Args:
      url (str):  URL to the webpage that contains the metadata about the zip files.

  Returns:
      str: Webpage content as a string.
  """
  options = ChromeOptions()
  options.headless = True
  driver = Chrome(options=options)
  driver.get(url)

  table_id = "tbody-content"
  data = driver.find_element(By.ID, table_id)
  sleep(3)
  return data.text

def webpage_content_to_dict(webpage_content: str) -> dict:
  """ Converts webpage content (from string) to a dictionary.

  Args:
      webpage_content (str): A long string where each line contains (filename, last_modified_date, filesize) separated by " ".

  Returns:
      dict: A dictionary containing (filename, last_modified_date, filesize) as keys with respective List[str] as values.
  """
  data : dict = {"filename": [], "last_modified_date": [], "filesize": []}
  table_content : List[str] = webpage_content.split("\n")
  for row in table_content:
    if row[:6].isdigit():
      filename, last_modified_date, filesize = extract_row_content(row)
      data["filename"].append(filename)
      data["last_modified_date"].append(last_modified_date)
      data["filesize"].append(filesize)
  return data 

def save_metadata(data : dict, data_directory: str, filename : str) -> None:
  """Saves data dict as csv file in the data_directory.

  Args:
      data (dict): A dictionary containing (filename, last_modified_date, filesize) as keys with respective List[str] as values.
      data_directory (str): A path where all of the data are located.
      filename (str): Name of the file.
  """
  pd.DataFrame.from_dict(data).to_csv(get_path(data_directory, filename), index= False)

def update_metadata(data : dict, data_directory : str, filename: str) -> None: 
  """ Modifies the meta data on file if it differs from the data. 

  Args:
      data (dict): A dictionary containing (filename, last_modified_date, filesize) as keys with respective List[str] as values.
      data_directory (str): A path where all of the data are located
      filename (str): Name of the metadata on file.
  """
  filepath : str = get_path(data_directory, filename)
  filenames, modified_dates, filesize = list(data.values())
  rows = zip(filenames, modified_dates, filesize)

  zipfile_metadata : pd.DataFrame = pd.read_csv(filepath, parse_dates = ["last_modified_date"])

  for row in rows:
    name, date, size = row
    if zipfile_metadata.loc[zipfile_metadata["filename"] == name].shape[0] > 0:
      logged_date : datetime = pd.Timestamp(zipfile_metadata.loc[zipfile_metadata["filename"] == name, "last_modified_date"].values[0]).to_pydatetime()
      if date > logged_date:
        zipfile_metadata.loc[zipfile_metadata["filename"] == name] = [name, date, size]

    else:
      zipfile_metadata.loc[len(zipfile_metadata)] = [name, date, size]

  save_metadata(zipfile_metadata.to_dict(orient="list"), data_directory, filename)


def extract_row_content(row : str) -> tuple[str, datetime, str]:
  """ Extracts the filename, last_modified_date, and filesize from row.

  Args:
      row (str): A single string containing metadata information about a downloadable zip file.

  Returns:
      tuple[str, datetime, str]: A tuple containing the name of the file, last modified date, and the filesize
  """
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

def archive_data(data_directory : str, filename: str) -> None:
  """Moves filename to the archive folder and keeps tracks the version of the file.

  Args:
      data_directory (str): A path where all of the data are located
      filename (str): Name of the file.
  """
  year : str = filename[:4]
  from_path : str = get_path(data_directory, year, filename)
  archive_path : str = get_path(data_directory, "archive")
  files_in_archive : List[str] = [file for file in os.listdir(archive_path) if file[:-4] == ".csv"]
  if filename in files_in_archive:
    j = len(filename[:-4]) # extracts name part of the filename
    version_num = len([f for f in files_in_archive if filename[:j] == f[:j]])
    new_filename = f"{filename[:j]}_{version_num + 1}.csv"
    to_path = get_path(archive_path, new_filename, include_dir=False)
    shutil.make_move(from_path, to_path)
  
  else:
    to_path : str = get_path(archive_path, filename, include_dir=False)
    shutil.move(from_path, to_path)

def extract_zipfile(url: str = "https://divvy-tripdata.s3.amazonaws.com", data_directory: str = "data", destination : str = "raw", metadata_filename : str = "zipfile_metadata.csv") -> None:
  """Downloads and extracts all content form zip file from url if the content doesn't exist in the data directory. 

  Args:
      url (str, optional): Webpage where the downloadable zipfile are located. Defaults to "https://divvy-tripdata.s3.amazonaws.com".
      data_directory (str, optional): Name of the directory where all of the data is stored. Defaults to "data".
      destination (str, optional): _description_. Defaults to "raw".
      metadata_filename (str, optional): Name of the metadata filename. Defaults to "zipfile_metadata.csv".
  """
  extract_zipfile_metadata(data_directory, metadata_filename) 
  filepath : str = get_path(data_directory, metadata_filename)
  metadata : pd.DataFrame = pd.read_csv(filepath, parse_dates=["last_modified_date"])

  zipfile_filenames = list(metadata.filename)
  source_and_filename = [(f"{url}/{filename}", f"{filename[:-4]}.csv") for filename in zipfile_filenames]

  for source, filename in source_and_filename:
    file, _ = urllib.request.urlretrieve(source)
    with zipfile.ZipFile(file) as zipfile_content:
      year : str = filename[:4]
      path : str = get_path(data_directory, destination, year)
      # if the path exists
      if os.path.isdir(path):
        # make sure sure file content does not exist
        if filename not in [f for f in os.listdir(path)]:
          print(f"Downloading {filename} to {path}")
          zipfile_content.extractall(path)
        else:
          # skip file i.e file already exist 
          print(f"{filename} already exists in {path}")
      else:
          print(f"Downloading {filename} to {path}")
          zipfile_content.extractall(path)

def get_path(*paths : tuple[str], include_dir : bool = True) -> str:
  """ Creates path from the source directory if the include_dir is True; Otherwise,
  creates path from the provided inp

  Args:
      include_dir (bool, optional): _description_. Defaults to True.

  Returns:
      str: An absolute path to desired location.
  """
  if include_dir:
    DIRECTORY : str = Path.cwd()
    return os.path.join(DIRECTORY, *paths)
  else:
    return os.path.join(*paths)