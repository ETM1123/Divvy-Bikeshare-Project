"""This file tests the methods form extract.Zipfile module"""
from extract.extract import Zipfile 
from helpers.util import add_file, delete_file, get_path, test_data_directory
import pytest
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


