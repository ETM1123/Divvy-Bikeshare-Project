# Testing
import os
import shutil
import pandas as pd


def create_path(path: str) -> None:
    """Create directory if it does not exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def create_files_in_directory(directory: str, file_ext: str, file_content: list[str], num_files: int) -> None:
    """
    Creates files with given file extension in the specified directory.
    Each file created will be named as file0, file1, file2,...fileN.

    Args:
      directory (str): Directory where files will be created.
      file_ext (str): Extension for the files to be created.
      num_files (int): Number of files to be created.
    """
    create_path(directory)
    if len(file_content) == num_files:
        for i in range(num_files):
            filename = os.path.join(directory, f"file{i}{file_ext}")
            with open(filename, 'w') as f:
                f.write(file_content[i])


def clear_directory(dir_path: str) -> None:
    """Removes all files and directories in the provided directory. This will
    permanently delete All content in provide directory - use with cation
    """

    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")


def add_5(col: str, row: pd.Series) -> int:
    return row[col] + 5   # type: ignore
