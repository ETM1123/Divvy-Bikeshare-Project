""" 
In this file we - we are going to create helper functions that will help us scrape data from the webpage.
"""

# imports 

URL : str  = "https://divvy-tripdata.s3.amazonaws.com/index.html"

# Idea 
# Extract information from the URL
# The webpage contains a table with the following content:
# Downloadable zipfile link, the last date  content of the zipfile was modified, and the size of the zip file

# We are not interested in all of the zip files on the webpage - we are only interested 
# in the zip files with the following name conventions 202004-divvy-tripdata.zip 
# i.e year+month-company name 

# We want to extract all of the information 

def get_zipfile_information(url: str, destination: str) -> None:
  """Extracts and saves the zipfile name, last modified date, and zipfile size information from the 
  table located on the url page as a csv file at destination location.

  args:
    url (str): link to the webpage where the content is stored in html and java script format.
    destination (str): Path to the location where to store data.
  """
  pass


def extract_zipfile_to(destination: str) -> None:
  """Downloads and unzips the zip file content to destination

  Args:
      destination (str): Path to the location where to store data. 
  """
  pass



