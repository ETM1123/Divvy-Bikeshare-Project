import os
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

if __name__ == "__main__":
  print(test_data_directory)