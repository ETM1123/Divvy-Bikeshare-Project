"""This file test the methods from extract.Metadata module"""

from datetime import datetime
from extract.extract import Metadata
import pandas as pd
import pytest

test_metadata = Metadata(directory = "test")
 
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

@pytest.mark.skip(reason="Not implemented")
def test_extract_row_content() -> None:
  raise NotImplementedError
@pytest.mark.skip(reason="Not implemented")
def test_get_data() -> None:
  raise NotImplementedError
@pytest.mark.skip(reason="Not implemented")
def test_archive_data() -> None:
  raise NotImplementedError
@pytest.mark.skip(reason="Not implemented")
def test_update() -> None:
  raise NotImplementedError
@pytest.mark.skip(reason="Not implemented")
def convert_to_dict() -> None:
  raise NotImplementedError
@pytest.mark.skip(reason="Not implemented")
def convert_to_csv() -> None:
  raise NotImplementedError