"""This file test the methods from extract.Metadata module"""

from datetime import datetime
import os
from extract.extract import Metadata
import pandas as pd
import pytest
from pathlib import Path

data_directory = os.path.join(str(Path(__file__).parents[1]), "data")
test_metadata = Metadata(directory = data_directory, filename="test_metadata_file.csv")
 
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
  print(data_directory)
  files_data_dir = [file for file in os.listdir(data_directory) if file[-4:] == ".csv"]
  print(files_data_dir)
  assert test_metadata.filename in files_data_dir, "csv file was not created"

@pytest.mark.skip(reason="Not implemented")
def test_get_data() -> None:
  raise NotImplementedError
@pytest.mark.skip(reason="Not implemented")
def test_archive_data() -> None:
  raise NotImplementedError
@pytest.mark.skip(reason="Not implemented")
def test_update() -> None:
  raise NotImplementedError