""" This file is going to contain the refactored code from util.py"""
from datetime import datetime
import os
import shutil
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
  
  def file_exist(self) -> bool:
    file_path = os.path.join(self.directory, self.filename)
    return os.path.isfile(file_path)

  def fetch_data(self) -> str:
    raise NotImplementedError
  
  def get_data(self) -> pd.DataFrame:
    """Returns a data frame if the metadata is extracted and stored as a csv file otherwise raise 
    a NameError"""
    file_path = os.path.join(self.directory, self.filename)
    if self.file_exist():
      return pd.read_csv(file_path, parse_dates=["last_modified_date"])
    raise ValueError(f"{self.filename} does not exist - need to invoke the Metadata.extract function first.")

  def archive_data(self, from_full_path : str,  filename : str, to_dir: str = "archive") -> None:
    """Moves csv file to archive folder."""
    to_full_path : str = self.get_file_to_full_path(filename, to_dir)
    shutil.move(from_full_path, to_full_path)

  def get_file_to_full_path(self, filename, to_dir):
    """Returns the address (str) if filename gets moved to_dir if filename exists
    in to_dir then it updates the address to track the different versions of filename in to_dir"""    
    to_dir_path = os.path.join(self.directory, to_dir)
    csv_files_in_to_dir = os.listdir(to_dir_path) 
    if filename in csv_files_in_to_dir:
      name_length : int = len(filename[:-4])
      version_num : int = len([file for file in csv_files_in_to_dir if file[:name_length] == filename[:name_length]])
      new_filename : str = f"{filename[:name_length]}_v{version_num + 1}.csv"
      return os.path.join(to_dir_path, new_filename)
    else:
      return os.path.join(to_dir_path, filename)
  
  def get_file_from_full_path(self, filename : str, from_dir : str = "raw") -> str:
    year = filename[:4]
    return os.path.join(self.directory, from_dir, year, filename)

  def update(self, new_data : dict[str, list[str]]) -> None:
    """Updates the content in the metadata csv file. If the new data contains the 
    same zipfile name and the data in the zipfile has been modified, then archive the content of the zipfile (i.e associated csv file)
    into the archive folder prior to updating the metadata file."""
    filepath : str = os.path.join(self.directory, self.filename)
    current_data : pd.DataFrame = pd.read_csv(filepath, parse_dates=["last_modified_date"])
    new_data_rows : list[tuple[str, datetime, str]] = list(zip(*new_data.values()))
    for row in new_data_rows:
      new_filename, new_last_modified_date, new_filesize = row
      if current_data.loc[current_data["filename"] == new_filename].shape[0] > 0:
        current_last_modified_date : datetime = pd.Timestamp(current_data.loc[current_data["filename"] == new_filename, "last_modified_date"].values[0]).to_pydatetime()
        if new_last_modified_date > current_last_modified_date:
          filename : str = f"{new_filename[:-4]}.csv"
          from_full_path : str = self.get_file_from_full_path(filename)
          self.archive_data(from_full_path, filename)
          current_data.loc[current_data["filename"] == new_filename] = [new_filename, new_last_modified_date, new_filesize]
      else:
        current_data.loc[len(current_data)] = [new_filename, new_last_modified_date, new_filesize]
    # Save modifications 
    self.convert_to_csv(current_data.to_dict(orient="list"))

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
