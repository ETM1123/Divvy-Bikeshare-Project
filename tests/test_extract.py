"""This script contains test modules for all the functions in extract.py file"""

import os
from typing import Union
import pytest
from etl.extract import download_file_from_web, extract_all_files_in_directory
from helpers.util_tests import create_files_in_directory
import pandas as pd
import subprocess
import tempfile


def test_download_file_from_web_valid_url():

    test_compressed_file: dict[str, dict[str, Union[str, bool]]] = {
        'input': {
            'url': 'https://divvy-tripdata.s3.amazonaws.com/202004-divvy-tripdata.zip',
            'filename': '202004-divvy-tripdata.zip',
            'compressed': True
        }
    }
    test_non_compressed_file: dict[str, dict[str, Union[str, bool]]] = {
        'input': {
            'url': 'https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=GeoJSON',
            'filename': 'neighborhood.geojson',
            'compressed': False
        }

    }

    test_cases: list[dict[str, dict[str, Union[str, bool]]]] = [test_compressed_file, test_non_compressed_file]

    for test_case in test_cases:
        with tempfile.TemporaryDirectory() as temp_dir:
            url, filename, compressed = test_case['input'].values()
            download_file_from_web(url, filename, temp_dir, compressed)   # type: ignore
            if compressed:
                assert os.path.exists(os.path.join(temp_dir, filename.replace(".zip", ".csv")))   # type: ignore
            else:
                assert os.path.exists(os.path.join(temp_dir, filename))   # type: ignore


def test_download_file_from_web_error():
    with pytest.raises(subprocess.CalledProcessError):
        with tempfile.TemporaryDirectory() as temp_dir:
            download_file_from_web("www.fake-url-should-fail.com", "bad_file.csv", temp_dir)


def test_extract_all_files_in_directory() -> None:
    """
    Test that the function returns a concatenated DataFrame containing data from all matching files.
    """

    file_content: list[str] = [
        'col1,col2\n1,a\n2,b\n3,c\n',
        'col1,col2\n4,d\n5,e\n6,f\n',
        'col1,col2\n7,g\n8,h\n9,i\n',
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        create_files_in_directory(temp_dir, file_ext='.csv', file_content=file_content, num_files=len(file_content))

        combined_df = extract_all_files_in_directory(temp_dir, file_ext='.csv')
        expected_df = pd.concat([pd.read_csv(os.path.join(temp_dir, file))
                                for file in os.listdir(temp_dir) if file.endswith('.csv')], axis=0, ignore_index=True)

        pd.testing.assert_frame_equal(combined_df, expected_df)
