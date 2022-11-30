"""This file tests the methods form extract.Zipfile module"""
from extract.extract import Zipfile 
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

@pytest.mark.skip(reason="Not implemented.")
def test_file_exists() -> None:
  pass
