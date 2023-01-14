import os
import zipfile
from pathlib import Path

test_data_directory = os.path.join(str(Path(__file__).parents[2]), "data", "testfiles")

def add_file(filename, year_N= True) -> None:
  if year_N:
    year = filename[:4]
    dir_path = os.path.join(test_data_directory, "raw", year)
    path = os.path.join(test_data_directory, "raw", year, filename)
  else:
    dir_path = test_data_directory
    path = os.path.join(dir_path, filename)
  
  try:
    open(path, "w").close()

  except FileNotFoundError:
    os.makedirs(dir_path)
    open(path, "w").close()

  except FileExistsError:
    print(f"File: {filename} already exist in {dir_path} directory")

  print(f"done adding {path}")

def delete_file(path) -> None:
  if os.path.isfile(path):
    os.remove(path)

def get_path(*path, include_home_dir = True):
  if include_home_dir:
    return os.path.join(test_data_directory, *path)
  else:
    return os.path.join(*path)

def create_zipfile(zip_file_path, dummy_file_path):
  # filepaths 
  # zip_file_path : str  = os.path.join(test_data_directory, zipfile_name)
  # dummy_file_path : str = os.path.join(test_data_directory, dummy_file_name)
  # create dummy text file
  dummy_file_name = "file1.txt" 
  create_dummy_file(dummy_file_path)

  # create a ZipFile object
  zip_file = zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED)
  compression = zipfile.ZIP_DEFLATED
  try:
    # add a file to the zip file
    zip_file.write(dummy_file_path, dummy_file_name, compress_type=compression)
  except FileNotFoundError:
    print("Dummy file was created")
  finally:
    zip_file.close()
    os.remove(dummy_file_path)


def create_dummy_file(file_name : str) -> None:
  # Open the file in write mode
  file = open(file_name, "w")
  # Write some text to the file
  file.write("test 1,2,3 ...")
  # Close the file
  file.close()  


def test_download_zipfile() -> None:
  # Set up:
  zip_file_name = "files.zip"
  zip_file_path = os.path.join(test_data_directory, zip_file_name)

  # Create a dummy text file
  dummy_file_name = "file1.txt" 
  dummy_file_path : str = os.path.join(test_data_directory, dummy_file_name)
  create_dummy_file(dummy_file_path)
  
  # Compression type

if __name__ == "__main__":
  print(test_data_directory)