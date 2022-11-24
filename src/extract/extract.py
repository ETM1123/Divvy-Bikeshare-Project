""" This file is going to contain the refactored code from util.py"""
from datetime import datetime
import pandas as pd

class Zipfile:
  """Downloads zipfile from web address and stores zipfile content locally"""
  pass


class Metadata:
  """Extracts zipfile's metadata from web address and saves it as a csv file on local computer"""
  METADATA_URL : str = ""
  METADATA_TABLE_ID : str = ""
  Min_ROW_LENGTH : int = 67
  def __inti__(self, directory : str, filename : str = "zipfile_metadata.csv"):
    self.directory = directory
    self.filename = filename

  def extract(self):
   raise NotImplementedError

  def extract_row_content(self, row: str) -> tuple[str, datetime, str]:
    raise NotImplementedError

  def fetch_data(self) -> str:
    raise NotImplementedError
  
  def get_data(self) -> pd.DataFrame:
    raise NotImplementedError

  def archive_data(self, from_full_path : str, to_dir: str, filename : str) -> None:
    raise NotImplementedError

  def update(self, data : dict[str, list[str]]) -> None:
    raise NotImplementedError

  def convert_to_dict(self, data : str):
    raise NotImplementedError

  def convert_to_csv(self, data : dict[str : list]) -> None: 
    raise NotImplementedError
    