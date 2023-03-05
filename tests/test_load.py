from pathlib import Path
from load.load import send_to_csv
from helpers.util import clear_directory, create_files_in_directory
import pandas as pd
import os

TEST_DIR = os.path.join(str(Path(__file__).parents[1]), "data", "test")

def test_send_to_csv():
  # Set up
  clear_directory(TEST_DIR)
  data = {'col1': ['a', 'b', 'c'], 'col2': [1, 2, 3]}
  df = pd.DataFrame(data)

  # Call 
  filename = 'test.csv'
  send_to_csv(df, filename, TEST_DIR)

  # Verify 
  filepath = os.path.join(TEST_DIR, filename)
  assert os.path.exists(filepath)
  expected_content = 'col1,col2\na,1\nb,2\nc,3\n'
  with open(filepath, 'r') as f:
    assert f.read() == expected_content
      
  # Clean up
  clear_directory(TEST_DIR)


# if __name__ == "__main__":
#   print(destination)
