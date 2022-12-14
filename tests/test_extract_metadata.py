"""This file test the methods from extract.Metadata module"""

from datetime import datetime
import os
from extract.extract import Metadata
import pandas as pd
import pytest
from pathlib import Path
from helpers.util import add_file, delete_file, test_data_directory

test_metadata = Metadata(directory = test_data_directory, filename="test_metadata_file.csv")
 
def test_valid_row() -> None:
  test_cases : dict[str, tuple[bool, str]] = {
  "202001-CCCC-CCCCCCCC.zip Jan 1st 2020, 10:00:00 am 1.11 MB ZIP file" : (True, "Valid row"),
  "Jan 1st 2020, 10:00:00 am 1.11 MB ZIP file" : (False, "Missing filename"),
  "202001-CCCC-CCCCCCCC.zip 10:00:00 am 1.11 MB ZIP file" : (False, "Missing date info"),
  "202001-CCCC-CCCCCCCC.zip Jan 1st 2020 1.11 MB ZIP file" : (False, "Missing time info"),
  "202001-CCCC-CCCCCCCC.zip Jan 1st 2020, 10:00:00 am" : (False, "Missing file size info"),
  "" : (False, "missing everything"),
  "202001-CCCC-CCCCCCCC.csv Jan 1st 2020, 10:00:00 am 1.11 MB ZIP file" : (False, "Filename has incorrect extension"),
  "asdfgh-fghj-sdkfhdfj.zip XXX 999 2xc0, 1e:0s:04 ax 1.11 MB ZIP file" : (False, "Gibberish"),
  }

  for test_case, output in test_cases.items():
    actual_output = test_metadata.valid_row(test_case)
    expected_output, err_message = output
    assert expected_output == actual_output, err_message

def test_extract_row_content() -> None:
  test_cases = {
  "202001-CCCC-CCCCCCCC.zip Jan 1st 2020, 10:00:00 am 1.11 MB ZIP file" : (("202001-CCCC-CCCCCCCC.zip", datetime.strptime("Jan 1 2020 10:00AM", "%b %d %Y %I:%M%p"), "1.11 MB"), "Correct format - beginning of the Year"),
  "202012-CCCC-CCCCCCCC.zip Dec 31st 2020, 10:00:00 am 1.11 MB ZIP file" : (("202012-CCCC-CCCCCCCC.zip", datetime.strptime("Dec 31 2020 10:00AM", "%b %d %Y %I:%M%p"), "1.11 MB"), "Correct format - end of the year."),
  "202001-CCCC-CCCCCCCC.zip Jan 1st 2020, 12:59:00 pm 1.11 MB ZIP file" : (("202001-CCCC-CCCCCCCC.zip", datetime.strptime("Jan 1 2020 12:59PM", "%b %d %Y %I:%M%p"), "1.11 MB"), "Correct format - Hour changed"),
  "202001-CCCC-CCCCCCCC.zip Jan 1st 2020, 10:00:00 am 10.11 MB ZIP file" : (("202001-CCCC-CCCCCCCC.zip", datetime.strptime("Jan 1 2020 10:00AM", "%b %d %Y %I:%M%p"), "10.11 MB"), "Correct format - large filesize"),
  }
  for test_input, output in test_cases.items():
    actual_output = test_metadata.extract_row_content(test_input)
    expected_output, err_message = output # all cases should pass i.e they're all in the correct format
    assert expected_output == actual_output, err_message

def test_convert_to_dict() -> None:
  test_cases : dict[str, dict] = {
  "202001-CCCC-CCCCCCCC.zip Jan 1st 2020, 10:00:00 am 1.11 MB ZIP file" : { "filename" : ["202001-CCCC-CCCCCCCC.zip"], "last_modified_date" : [datetime.strptime("Jan 1 2020 10:00AM", "%b %d %Y %I:%M%p")],"filesize" :  ["1.11 MB"]},
  "": {"filename" : [], "last_modified_date" : [], "filesize" : []}
  }

  for test_input, expected_output in test_cases.items():
    actual_output = test_metadata.convert_to_dict(test_input)
    assert expected_output == actual_output

def test_convert_to_csv() -> None:
  test_case : dict[str, list] = { 
    "filename" : ["202001-CCCC-CCCCCCCC.zip", "202101-CCCC-CCCCCCCC.zip", "202201-CCCC-CCCCCCCC.zip"], 
    "last_modified_date" : [datetime.strptime("Jan 1 2020 10:00AM", "%b %d %Y %I:%M%p"), datetime.strptime("Jan 2 2021 10:00AM", "%b %d %Y %I:%M%p"), datetime.strptime("Jan 3 2022 10:00AM", "%b %d %Y %I:%M%p")],
    "filesize" :  ["1.11 MB", "10.11 MB", "100.11 MB"]
    }
  test_metadata.convert_to_csv(test_case) # should create a csv file
  print(test_data_directory)
  files_data_dir = [file for file in os.listdir(test_data_directory) if file[-4:] == ".csv"]
  print(files_data_dir)
  assert test_metadata.filename in files_data_dir, "csv file was not created"

def test_get_data() -> None:
  test_cases = [
  Metadata(directory = test_data_directory, filename="test_metadata_file.csv"), # file exists
  Metadata(directory = test_data_directory, filename="file_does_not_exist.csv") # file does not exist
  ]
  with pytest.raises(ValueError):
    for i, test_case in enumerate(test_cases):
      actual_output = test_case.get_data()
      if i == 0:
        path = os.path.join(test_data_directory, test_case.filename)
        expected_output = pd.read_csv(path)
        assert expected_output == actual_output, "DataFrames are diff"
      # i = 2 should raise a Value Error

def test_get_from_full_path() -> None:
  test_cases : dict[str, str] = {
    "202001-TEST-FILE0001.csv" : os.path.join(test_data_directory, "raw", "2020","202001-TEST-FILE0001.csv"),
    "202001-TEST-FILE0001.txt" : os.path.join(test_data_directory, "raw", "2020","202001-TEST-FILE0001.txt")
  }
  for test_input, expected_output in test_cases.items():
    add_file(test_input)
    actual_output = test_metadata.get_file_from_full_path(test_input)
    msg = f"Expected_output: {expected_output} \n Actual_output: {actual_output}"
    
    assert expected_output == actual_output, msg
    delete_file(actual_output)

def test_get_to_full_path() -> None:
  test_cases : dict[str, str] = {
    "202001-TEST-FILE0001.csv" : os.path.join(test_data_directory, "raw", "2020", "202001-TEST-FILE0001_v2.csv"), # in file
    "202001-TEST-FILE0001.txt" : os.path.join(test_data_directory, "raw","2020", "202001-TEST-FILE0001.txt") # not in file
  }
  dir = os.path.join("raw", "2020")
  add_file("202001-TEST-FILE0001.csv")
  for test_input, expected_output in test_cases.items():
    actual_output = test_metadata.get_file_to_full_path(test_input, to_dir= dir)  
    assert expected_output == actual_output, actual_output

def test_archive_data() -> None: 
  test_filename : str = "202001-TEST-FILE000.csv"
  from_dir_path : str = os.path.join(test_data_directory, "raw", "2020")
  from_full_path : str = os.path.join(from_dir_path, test_filename)
  # to_dir : str = os.path.join("testfiles", "archive")
  to_dir_full : str = os.path.join(test_data_directory, "archive")

  for i in range(2):
    add_file(test_filename)
    test_metadata.archive_data(from_full_path,test_filename)
    file_is_not_org_path : bool = test_filename not in os.listdir(from_dir_path)
    if i == 0:
      file_is_archived : bool = test_filename in os.listdir(to_dir_full)

    if i == 1: 
      filename_v2 = f"{test_filename[:-4]}_v2.csv" # should track version
      file_is_archived : bool = filename_v2 in os.listdir(to_dir_full)

    assert file_is_not_org_path and file_is_archived
  # clean up (comment code below if you want files to be created)
  # for file in os.listdir(to_dir_full):
  #   path : str = os.path.join(to_dir_full, file)
  #   delete_file(path)

def test_update() -> None:
  test_metadata_csv = """filename,last_modified_date,filesize
202001-CCC1-CCCCCCCC.zip,2020-01-01 10:00:00,1.11 MB
202102-CCC2-CCCCCCCC.zip,2021-02-02 10:00:00,10.11 MB
202203-CCC3-CCCCCCCC.zip,2022-03-03 10:00:00,100.11 MB
"""
  filename : str = "test_metadata.csv"
  # path =  os.path.join(test_data_directory, "testfiles")
  full_file_path = os.path.join(test_data_directory, filename)
  with open(full_file_path, "w") as file:
    file.write(test_metadata_csv)

  new_data : dict[str, list[tuple(str, datetime, str)]] = {
    "filename" : ["202204-CCC4-CCCCCCCC.zip", "202001-CCC1-CCCCCCCC.zip"],
    "last_modified_date" : [datetime.strptime("Apr 4 2020 10:00AM", "%b %d %Y %I:%M%p"), datetime.strptime("Jan 21 2020 10:00AM", "%b %d %Y %I:%M%p") ],
    "filesize" : ["1000.11 MB", "100.11 MB"]
  }


  expected_dict : dict[str, list] = { 
    "filename" : ["202001-CCC1-CCCCCCCC.zip", 
                  "202102-CCC2-CCCCCCCC.zip", 
                  "202203-CCC3-CCCCCCCC.zip",
                  "202204-CCC4-CCCCCCCC.zip"], 
    "last_modified_date" : [datetime.strptime("Jan 21 2020 10:00AM", "%b %d %Y %I:%M%p"), 
                            datetime.strptime("Feb 2 2021 10:00AM", "%b %d %Y %I:%M%p"), 
                            datetime.strptime("Mar 3 2022 10:00AM", "%b %d %Y %I:%M%p"),
                            datetime.strptime("Apr 4 2020 10:00AM", "%b %d %Y %I:%M%p")],
    "filesize" :  ["100.11 MB", "10.11 MB", "100.11 MB", "1000.11 MB"]
    }
  # add the modified file
  add_file("202001-CCC1-CCCCCCCC.csv")

  # expected result 
  expected_result : pd.DataFrame = pd.DataFrame.from_dict(expected_dict)

  # invoke 
  metadata = Metadata(directory=test_data_directory, filename = filename)
  metadata.update(new_data)
  actual_result_df = metadata.get_data()

  assert expected_result.equals(actual_result_df)