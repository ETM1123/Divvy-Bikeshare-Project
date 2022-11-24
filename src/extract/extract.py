""" This file is going to contain the refactored code from util.py"""
from datetime import datetime
import os
import pandas as pd

class Zipfile:
  """Downloads zipfile from web address and stores zipfile content locally"""
  pass


class Metadata:
  """Extracts zipfile's metadata from web address and saves it as a csv file on local computer"""
  METADATA_URL : str = ""
  METADATA_TABLE_ID : str = ""
  Min_ROW_LENGTH : int = 67

  def __init__(self, directory : str, filename : str = "zipfile_metadata.csv") -> None:
    self.directory = directory
    self.filename = filename

  def extract(self):
   raise NotImplementedError

  def extract_row_content(self, row: str) -> tuple[str, datetime, str]:
    """Extracts the filename, last modified date, and the file size if and only if the provided row is correctly formatted"""
    row_contents : list[str] = row.split(" ")
    
    month, day, year = row_contents[1], row_contents[2][:-2], row_contents[3][:-1]
    hour_and_minute, am_pm  = ":".join(row_contents[4].split(":")[:-1]), row_contents[5].upper()
    time = "".join([hour_and_minute, am_pm])

    filename : str = row_contents[0]
    last_modified_date : datetime = datetime.strptime(" ".join([month, day, year, time]), "%b %d %Y %I:%M%p")
    filesize : str = " ".join(row_contents[6:8])
    return filename, last_modified_date, filesize

  def valid_row(self, row : str) -> bool:
    """Verifies row if it's in the correct format to extract content from."""
    if len(row) < self.Min_ROW_LENGTH: return False
    if ".zip" not in row: return False
    if not row[:6].isdigit(): return False
    return True

  def fetch_data(self) -> str:
    raise NotImplementedError
  
  def get_data(self) -> pd.DataFrame:
    raise NotImplementedError

  def archive_data(self, from_full_path : str, to_dir: str, filename : str) -> None:
    raise NotImplementedError

  def update(self, data : dict[str, list[str]]) -> None:
    raise NotImplementedError

  def convert_to_dict(self, data : str) -> dict:
    """Converts the correctly formatted lines in data (i.e row) into a dict. NOTE if there exist some line in the data where
    the format does not suffice then the line wll not be in the dict."""
    data_dict : dict[str : list] = {"filename" : [], "last_modified_date" : [], "filesize" : []}
    rows : list[str] = data.split("\n")
    for row in rows:
      if self.valid_row(row):
        filename, last_modified_date, filesize = self.extract_row_content(row)
        data_dict["filename"].append(filename)
        data_dict["last_modified_date"].append(last_modified_date)
        data_dict["filesize"].append(filesize)
    return data_dict

  def convert_to_csv(self, data : dict[str : list]) -> None: 
    """Converts data (in dict format: (column names : values) where the list of values is the same length for all column names.
    The file is saved in the data directory."""
    file_location: str = os.path.join(self.directory, self.filename)
    data = pd.DataFrame.from_dict(data)
    print(data.head())
    data.to_csv(file_location, index=False)
