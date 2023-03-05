import os
import pandas as pd


def send_to_csv(df: pd.DataFrame, filename: str, destination: str) -> None:
    filepath = os.path.join(destination, filename)
    df.to_csv(filepath, index=False)
