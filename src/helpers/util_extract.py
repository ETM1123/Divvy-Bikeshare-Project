from datetime import datetime
import os
from dateutil.relativedelta import relativedelta
from etl.extract import download_file_from_web
from helpers.util_tests import create_path

URL_PREFIX = "https://divvy-tripdata.s3.amazonaws.com"
FILENAME = "divvy-tripdata.zip"


def extract_divvy_biketrip_dataset(start_date, end_date, destination, date_format: str = "%Y-%m-%d") -> None:
    """Extract the Divvy biketrip dataset for a given date range and save the files
    in a directory.

    Args:
      start_date (str): Start date for the range in the format "YYYY-MM-DD".
      end_date (str): End date for the range in the format "YYYY-MM-DD".
      destination (str): Directory where the extracted files should be saved.
      date_format (str, optional): Format of the date strings. Defaults to "%Y-%m-%d".

    Returns:
      None
    """
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)

    if validate_date(start_date, end_date):
        urls = get_data_urls(start_date, end_date)
        # Download data
        for url in urls:
            filename = url[-25:]
            year = filename[:4]
            path = os.path.join(destination, year)
            create_path(path)
            download_file_from_web(url, filename, path)
    else:
        print("Invalid Date")


def format_data_url(date_str: str) -> str:
    """Create the URL to download Divvy biketrip dataset for a given date.
    """
    return f"{URL_PREFIX}/{date_str}-{FILENAME}"


def get_data_urls(start_date, end_date):
    """Generate a list of URLs for downloading Divvy biketrip dataset
    for a range of dates.

    Args:
      start_date (datetime): Start date of the range.
      end_date (datetime): End date of the range.

    Returns:
      List of str: List of URLs for downloading the dataset.
    """
    dates = generate_dates(start_date, end_date)
    filenames = [format_data_url(date) for date in dates]
    return filenames


def generate_dates(start_date: datetime, end_date: datetime) -> list[str]:
    """Generate a list of date strings for a range of dates.

    Args:
      start_date (datetime): Start date of the range.
      end_date (datetime): End date of the range.

    Returns:
      list[str]: List of date strings in the format "YYYYMM".
    """
    dates = []
    current_date = start_date

    while current_date <= end_date:
        dates.append(current_date.strftime("%Y%m"))
        current_date += relativedelta(months=1)

    return dates


def validate_date(start_date: datetime, end_date: datetime):
    """Validate if the start and end dates for the Divvy biketrip dataset
    download are within a valid range.

    Args:
      start_date: Start date for the range.
      end_date: End date for the range.

    Returns:
      bool: True if dates are within valid range, False otherwise.
    """
    april_2020 = datetime(year=2020, month=4, day=1)  # min date
    current_date = datetime.now()  # max date
    return start_date >= april_2020 and end_date <= current_date
