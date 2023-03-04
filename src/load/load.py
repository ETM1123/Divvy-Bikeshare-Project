import os
import pandas as pd
import glob


def send_to_csv(df : pd.DataFrame, filename : str, destination : str) -> None:
  filepath = os.path.join(destination, filename)
  return df.to_csv(filepath, index=False)

