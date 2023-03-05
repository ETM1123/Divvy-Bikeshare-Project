import subprocess
import os
from typing import List
import pandas as pd
import glob


def download_file_from_web(url: str, filename: str, destination: str, compressed=True) -> None:
    """Download a file from a URL and save it in a destination directory.

    Args:
        url (str): URL to download the file.
        filename (str): Name of the file to save.
        destination (str): Directory where the file should be saved.
        compressed (bool, optional): Whether the file is compressed. Defaults to True.
    """
    try:
        data_path = os.path.join(destination, filename)
        # Download data
        subprocess.run(f'curl -sSL {url} > {data_path}', shell=True, check=True)
        if compressed:
            # Extract zipfile content
            subprocess.run(f'unzip -o {data_path} -d {destination}', shell=True, check=True)
            # Remove zipfile
            subprocess.run(f'rm {data_path}', shell=True, check=True)
    except subprocess.CalledProcessError as e:
        raise subprocess.CalledProcessError(e.returncode, "Invalid url")
        # print(f"Error: {e}")


def extract_all_files_in_directory(input_dir: str, file_ext: str = '.csv', sub_dir: List[str] = None, **kwargs) -> pd.DataFrame:
    """Reads all files with a matching file extension in a given directory and returns them as a Pandas DataFrame.

    Args:
      input_dir (str): The directory to search for files in.
      file_ext (str, optional): The file extension to search for. Defaults to '.csv'.
      sub_dir (list, optional): Specified sub-directories to search for files in. Defaults to None.

    Returns:
      pd.DataFrame: A concatenated Pandas DataFrame containing data from all matching files in the specified directory/sub-directories.
    """
    print("Starting to combine files together ... ")
    if sub_dir:
        df_list = [pd.read_csv(file, **kwargs) for dir in sub_dir for file in glob.glob(f"{input_dir}/{dir}/*{file_ext}") if file.endswith('.csv')]
    else:
        df_list = [pd.read_csv(file, **kwargs) for file in glob.glob(f"{input_dir}/*{file_ext}") if file.endswith(file_ext)]

    combined_df = pd.concat(df_list, axis=0, ignore_index=True)
    print("Finished combining files together")
    return combined_df
