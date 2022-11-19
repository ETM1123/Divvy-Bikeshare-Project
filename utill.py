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

# Idea 
# Extract information from the URL
# The webpage contains a table with the following content:
# Downloadable zipfile link, the last date  content of the zipfile was modified, and the size of the zip file

# We are not interested in all of the zip files on the webpage - we are only interested 
# in the zip files with the following name conventions 202004-divvy-tripdata.zip 
# i.e year+month-company name 

# We want to extract all of the information 

def get_zipfile_information(url: str, destination: str, metadata_filename : str = "zipfile_metadata.csv" ) -> None:
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


  # # Don't launch browser to webpage
  # options = ChromeOptions()
  # options.headless = True

  # # Set up driver 
  # driver = Chrome(options=options)
  # # Fetch webpage 
  # driver.get(url)

  # zipfile_info_data : dict = {"filename": [], "last_modified_date": [], "filesize": []}
  # table_id : str = "tbody-content"
  # table_data = driver.find_element(By.ID, table_id)
  # csv_filename : str = f"{destination}/zipfile_info.csv"

  # sleep(3)

  # table_content : List[str] = table_data.text.split("\n")
  # csv_files_in_destination : List[str] = [ file for file in os.listdir(destination) if file[-4:] == ".csv" ]
  # # Extract the filename from path
  # csv_file = csv_filename[len(destination) + 1:]

  # file_already_exists : bool = True if csv_file in csv_files_in_destination else False
  # zipfile_info = pd.read_csv(csv_file, parse_dates=["last_modified_date"]) if file_already_exists else None


  # for index, row in enumerate(table_content):
  #   valid_row : bool = row[:6].isdigit()

  #   if valid_row:
  #     filename, last_modified_date, file_size = extract_row_content(row)

  #     if file_already_exists:
  #       filename_exists_in_df : bool = zipfile_info.loc[zipfile_info["filename"] == filename].shape[0] > 0
  #       if filename_exists_in_df:
  #         # last_modified_date = datetime.strptime(last_modified_date, )
  #         current_logged_modified_date = pd.Timestamp(str(zipfile_info.loc[zipfile_info["filename"] == filename, "last_modified_date"].values[0])).to_pydatetime()
  #         if last_modified_date > current_logged_modified_date:
  #           # archive data 
  #           archive_data(filename)
  #           # update the modified date and file size 
  #           zipfile_info.loc[zipfile_info["filename"] == filename] = [filename, last_modified_date, file_size]
  #       else:
  #         # add row to data frame
  #         zipfile_info.loc[len(zipfile_info)] = [filename, last_modified_date, file_size]

  #     else:
  #       # add content to dictionary 
  #       zipfile_info_data["filename"].append(filename)
  #       zipfile_info_data["last_modified_date"].append(last_modified_date)
  #       zipfile_info_data["filesize"].append(file_size)

  # if len(list(zipfile_info_data.items())) > 0 and not file_already_exists:
  #   data : pd.DataFrame = pd.DataFrame.from_dict(zipfile_info_data)
  #   # Save extracted data as a csv file
  #   data.to_csv(csv_filename, index = False)

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
  pass

def save_metadata(data : dict, destination: str, filename : str) -> None:
  pass

def update_metadata(data : dict, destination : str, filename: str) -> None:
  pass

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

def archive_data(filename: str) -> None:
  pass

def extract_zipfile_to(destination: str) -> None:
  """Downloads and unzips the zip file content to destination

  Args:
      destination (str): Path to the location where to store data. 
  """
  pass

if __name__ == "__main__":
  URL : str  = "https://divvy-tripdata.s3.amazonaws.com/index.html"
  current_path : str = "/Users/eyobmanhardt/Desktop/divvy_bikeshare/divvy_project"
  get_zipfile_information(URL, current_path)


