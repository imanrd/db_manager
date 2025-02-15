import pandas as pd


class DataModel:
    time_column = "Gmt time"


def load_data(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)
