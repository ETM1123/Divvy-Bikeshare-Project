from typing import Callable, List, Union
import pandas as pd
import geopandas as gpd


def add_geo_field_from_lat_long(df: pd.DataFrame, geo_df: gpd.GeoDataFrame, x: str, y: str) -> pd.DataFrame:
    """
    Adds a geographic field to a pandas DataFrame based on latitude and longitude coordinates.

    Args:
        df (pd.DataFrame): The input DataFrame that contains the latitude and longitude columns.
        geo_df (gpd.GeoDataFrame): A GeoDataFrame that represents the geographical boundaries to which the coordinates will be matched.
        x (str): The name of the column in df that contains the longitude coordinates.
        y (str): The name of the column in df that contains the latitude coordinates.

    Returns:
        pd.DataFrame: A new DataFrame that includes the matched geographical features from geo_df based on the input latitude and longitude columns.
    """
    # create a GeoSeries of Point objects and convert to geopandas
    geometry = gpd.points_from_xy(df[x], df[y])
    df_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    # Set up correct projection
    df_gdf = df_gdf.set_crs(epsg=4326)
    geo_df = geo_df.set_crs(epsg=4326)
    # Join tables
    combined_gdf = df_gdf.sjoin(geo_df, how="inner", predicate='within').reset_index(drop=True)
    return pd.DataFrame(combined_gdf)


def combine_data(df1: pd.DataFrame, df2: pd.DataFrame, on: Union[str, List[str]], how: str = 'left') -> pd.DataFrame:
    """
    Combine two dataframes based on common columns.

    Args:
        df1 (pd.DataFrame): the first dataframe to be merged
        df2 (pd.DataFrame): the second dataframe to be merged
        on (str or list of str): the column(s) used to join the two dataframes
        how (str, optional): the type of join to be performed. Default is 'left'.
            Possible values: 'left', 'right', 'outer', 'inner'

    Returns:
        pd.DataFrame: a new dataframe with the merged data from the two input dataframes
    """
    return pd.merge(df1, df2, on=on, how=how)


def remove_column(df: pd.DataFrame, cols: Union[str, List[str]]) -> pd.DataFrame:
    """
    Remove one or more columns from a pandas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to remove columns from.
        cols (str or list of str): The name or names of the columns to remove.

    Returns:
        pd.DataFrame: The DataFrame with the specified columns removed.
    """
    return df.drop(cols, axis=1)


def select_column(df: pd.DataFrame, cols: Union[str, List[str]]) -> pd.DataFrame:
    """
    Select one or more columns from a pandas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to select columns from.
        cols (str or list of str): The name or names of the columns to select.

    Returns:
        pd.DataFrame: The DataFrame with only the specified columns.
    """
    cols = cols if type(cols) == list else [cols]
    return df[cols]


def filter_column(df: pd.DataFrame, column: str, condition: str, value: Union[int, float, str]):
    """
    Filters a dataframe based on a given column and condition.

    Args:
        df (pandas DataFrame): The dataframe to filter.
        column (str): The name of the column to filter.
        condition (str): The condition to filter on. Valid conditions are "=", "equal", ">", "greater", "<", "less".
        value (int, float, str): The value to filter on.

    Returns:
        pandas DataFrame: The filtered dataframe.

    Raises:
        ValueError: If the condition is not one of the valid conditions.
    """
    filtered_df = None
    match condition:
        case "=" | "equal":
            filtered_df = df.loc[df[column] == value, :]
        case ">" | "greater":
            filtered_df = df.loc[df[column] > value, :]
        case "<" | "less":
            filtered_df = df.loc[df[column] < value, :]
        case _:
            raise ValueError(f"Invalid condition '{condition}'. Valid conditions are '=', 'equal', '>', 'greater', '<', 'less'")
    return filtered_df


def sort_data(df: pd.DataFrame, by: str, order: str = "asc"):
    """Sort a DataFrame by a column and order.

    Args:
        df (pd.DataFrame): Input DataFrame.
        by (str): Column name to sort by.
        order (str, optional): Sort order, either "asc" or "desc". Default is "asc".

    Returns:
        pd.DataFrame: Sorted DataFrame.

    Raises:
        ValueError: If the condition is not one of the valid conditions.
    """
    # Check for valid order parameter
    if order.lower() not in ["asc", "desc"]:
        raise ValueError("Invalid order parameter, must be 'asc' or 'desc'")

    # Sort the DataFrame
    order = True if order.lower() == 'asc' else False
    sorted_df = df.sort_values(by=by, ascending=order)

    return sorted_df


def remove_duplicates(df: pd.DataFrame, subset: list[str], method: str = 'first') -> pd.DataFrame:
    """
    Remove duplicates from a pandas DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame to remove duplicates from.
        subset (list or tuple): List of column names to consider for identifying duplicates.
        method (str, optional): Method to use for removing duplicates. Can be 'first', 'last', or 'False'. Defaults to 'first'.

    Returns:
        pandas.DataFrame: DataFrame with duplicates removed.
    """
    return df.drop_duplicates(subset=subset, keep=method)


def remove_null_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with null (NaN) values from the given DataFrame.

    Args:
        df: A pandas DataFrame object containing the data to be filtered.

    Returns:
        pd.DataFrame: A new DataFrame object with the same columns as the input DataFrame, but
        with any rows containing null (NaN) values removed.
    """
    return df.dropna(axis=0, how='any', inplace=False)


def update_column_name(df: pd.DataFrame, col_name_mapping: dict[str, str]) -> pd.DataFrame:
    """
    Rename columns in a DataFrame based on the provided column name mappings.

    Args:
        df (pd.DataFrame): A pandas DataFrame that needs to have its column names updated.
        col_name_mapping (dict[str, str]): A dictionary where each key-value pair represents the old and new column names, respectively.

    Returns:
        pd.DataFrame: A new DataFrame with updated column names.
    """
    return df.rename(columns=col_name_mapping)


def add_column(df: pd.DataFrame, column_name: str, func: Callable, *args, **kwargs) -> pd.DataFrame:
    """
    Adds a new column to the given DataFrame by applying the given function to each row.

    Args:
        df (pd.DataFrame): The DataFrame to add the new column to.
        column_name (str): The name of the new column to add.
        func (callable): The function to apply to each row of the DataFrame.
        *args: Positional arguments to pass to the function `func`.
        **kwargs: Keyword arguments to pass to the function `func`.

    Returns:
        pd.DataFrame: A new DataFrame with the additional column added.
    """
    new_col = df.apply(lambda row: func(*args, **kwargs, row=row), axis=1)
    return pd.concat([df, new_col.rename(column_name)], axis=1)


def reset_index(df, drop=False):
    return df.reset_index(drop=drop)
