"""This file tests the methods form extract.Zipfile module"""
from extract.extract import Zipfile 
from helpers.util import add_file, delete_file, get_path, create_zipfile, test_data_directory
import pytest
import zipfile
import os
zf = Zipfile()

def test_get_source() -> None:
  test_cases = [
    (["a.csv", "b.txt", "hello"], 
     []),
    (["a.zip", "b.zip", "c.zip"],
     ["https://divvy-tripdata.s3.amazonaws.com/a.zip",
     "https://divvy-tripdata.s3.amazonaws.com/b.zip",
     "https://divvy-tripdata.s3.amazonaws.com/c.zip"])
  ]

  for test_input, expected_output in test_cases:
    actual_output = zf.get_source(test_input)
    assert sorted(expected_output) == sorted(actual_output)

def test_file_exists() -> None:
  test_cases : dict[str, bool] = {
    "test_file1.csv": True,
    "test_file2.txt": True,
    "test_file3.lol": False
  }
  for filename, expected_output in test_cases.items():
    if expected_output:
      add_file(filename, year_N=False)
    actual_output : bool = zf.file_exists(filename, test_data_directory)
    assert expected_output == actual_output
    # clean up
    file_path = get_path(filename, include_home_dir=True)
    delete_file(file_path)
    
def test_download_zipfile() -> None:
  # Set up:
  zip_file_name = "files.zip"
  zip_file_path = os.path.join(test_data_directory, zip_file_name)

  dummy_file_name = "file1.txt" 
  dummy_file_path : str = os.path.join(test_data_directory, dummy_file_name)

  # Create compressed zipfile with dummy file in it
  create_zipfile(zip_file_path, dummy_file_path)

  files_in_test_dir_before = os.listdir(test_data_directory)

  # Extract zipfile content 
  # i.e new files should be added to dir
  zf.download_zipfile(zip_file_path, dummy_file_name, test_data_directory)
  # Get filenames from dir
  files_in_test_dir_after = os.listdir(test_data_directory)
  # check the if content from zipfile extracted properly
  assert len(files_in_test_dir_after) > len(files_in_test_dir_before) and dummy_file_name in files_in_test_dir_after

  # remove compressed and extracted file
  os.remove(dummy_file_path)
  os.remove(zip_file_path)