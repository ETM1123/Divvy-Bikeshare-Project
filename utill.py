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

URL : str  = "https://divvy-tripdata.s3.amazonaws.com/index.html"

# Idea 
# Extract information from the URL
# The webpage contains a table with the following content:
# Downloadable zipfile link, the last date  content of the zipfile was modified, and the size of the zip file

# We are not interested in all of the zip files on the webpage - we are only interested 
# in the zip files with the following name conventions 202004-divvy-tripdata.zip 
# i.e year+month-company name 

# We want to extract all of the information 

def get_zipfile_information(url: str, destination: str) -> None:
  """Extracts and saves the name of the zip file, the last date it was modified, and the file size from the 
  table located on the url page. Saves the extracted content as a csv file in destination path.

  args:
    url (str): link to the webpage where the content is stored in html and java script format.
    destination (str): Path to the location where to store data.
  """
  # Don't launch browser to webpage
  options = ChromeOptions()
  options.headless = True

  # Set up driver 
  driver = Chrome(options=options)
  # Fetch webpage 
  driver.get(url)

  zipfile_info_data : dict = {"filename": [], "last modified date": [], "filesize": []}
  table_id : str = "tbody-content"
  table_data = driver.find_element(By.ID, table_id)

  sleep(3)

  table_content : List[str] = table_data.text.split("\n")

  for index, row in enumerate(table_content):
    row_content : List[str] = row.split(" ")
    filename : str = row_content[0]

    valid_row : bool = filename[:5].isdigit()

    if valid_row:
      print(f"row {index + 1} is valid! Extracting content ...")
      # Extract date information
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
      file_size = " ".join(row_content[6:8])

      # Add row content to dictionary 
      zipfile_info_data["filename"].append(filename)
      zipfile_info_data["last modified date"].append(last_modified_date)
      zipfile_info_data["filesize"].append(file_size)

      print(f"row {index + 1} extracted")

    else:
      continue

  data : pd.DataFrame = pd.DataFrame.from_dict(zipfile_info_data)

  # Save extracted data as a csv file
  csv_filename : str = f"{destination}/zipfile_info.csv"
  data.to_csv(csv_filename, index = False)

def extract_zipfile_to(destination: str) -> None:
  """Downloads and unzips the zip file content to destination

  Args:
      destination (str): Path to the location where to store data. 
  """
  pass



