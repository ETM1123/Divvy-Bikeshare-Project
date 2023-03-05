import pandas as pd
from etl.transform import *
import pytest

@pytest.fixture
def df() -> pd.DataFrame:
  df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 4, 6],'C' :['a', 'b', 'c']})
  return df

def test_add_column(df) -> None:
  def add_5(col, row) -> int:
    return row[col] + 5
  
  result = add_column(df, 'A_5', add_5, 'A')
  assert sorted(list(result['A_5'].values)) == [6, 7, 8]

def test_combine_data(df):
  df1 = pd.DataFrame({'A': [1, 2, 3], 'D': [3, 4, 5]})
  result = combine_data(df, df1, 'A')
  columns = list(result.columns)
  assert len(columns) == 4
  assert sorted(columns) == ['A', 'B', 'C', 'D']

def test_remove_column(df):
  result = remove_column(df, 'B')
  assert 'B' not in result.columns

def test_select_column(df):
  result = select_column(df, ['A', 'B'])
  assert sorted(list(result.columns)) == ['A', 'B']
  assert 'C' not in list(result.columns)

def test_filter_column(df):
  result = filter_column(df, 'B', 'equal', 4)
  assert len(result) == 2

def test_sort_data(df):
  result = sort_data(df, 'A', 'desc')
  assert (result['A'] == [3, 2, 1]).all()

def test_remove_duplicates(df):
  result = remove_duplicates(df, ['B'])
  assert len(result) == 2
