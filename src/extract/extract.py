import subprocess
import os
import pandas as pd
import glob

def download_file_from_web(url: str, filename: str, destination: str, compressed=True) -> None:
    """Download a file from a URL and save it in a destination directory.

    Args:
      url (str): URL to download the file.
      filename (str): Name of the file to save.
      destination (str): Directory where the file should be saved.
      compressed (bool, optional): Whether the file is compressed. Defaults to True.

    Returns:
      None
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
      print(f"Error: {e}")

def combine_csv_files(input_dir: str, output_file: str) -> None:
    """Combine all CSV files in a directory into a single CSV file.

    Args:
      input_dir (str): Directory containing the input CSV files.
      output_file (str): Name of the output CSV file.

    Returns:
      None
    """
    start_year, end_year = 2020, 2022
    df_list = []

    for year in range(start_year, end_year + 1):
      data_dir = os.path.join(input_dir, str(year))
      all_files = glob.glob(data_dir + "/*.csv")
      for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        table_name = filename.replace(".csv", "")
        df['table_name'] = table_name # keep track of the file
        df_list.append(df)
        print(f"added {filename} to df list")

    combined_df = pd.concat(df_list, axis=0, ignore_index=True)
    # Write the combined data to a single CSV file
    combined_df.to_csv(output_file, index=False)