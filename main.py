from extract.extract import download_file_from_web, extract_all_files_in_directory
from helpers.util import extract_divvy_biketrip_dataset, transform_data
from load.load import send_to_csv
from datetime import datetime
import os
from pathlib import Path


def main() -> None:
  # Extract variables
  start_date, end_date = "2020-04-01", "2022-12-01"
  dtype = {
    "ride_id" : str,
    "rideable_type" : str,
    "start_station_name" : str,
    "end_station_name" : str,
    "start_station_id" : str,
    "end_station_id" : str,
    "start_lat" : float,
    "start_lng" : float,
    "end_lat" : float,
    "end_lng" : float,
    "member_casual" : str,
    }

  date_parser  = lambda date: datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
  parse_dates : list[str] = ['started_at', 'ended_at']
  file_ext = ".csv" 
  sub_dir = ['2020', '2021', '2022']

  neighborhood_url ='https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=GeoJSON'
  state_url = 'https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip'

  neighborhood_filename = 'chicago_neighborhoods.geojson'
  state_filename = 'cb_2018_us_state_500k.zip'

  # Extract destinations 
  destination = os.path.join(str(Path(__file__).parents[0]), "data")
  divvy_intial_destination = os.path.join(destination, 'raw')
  neighborhood_intial_destination = os.path.join(destination, 'processed')
  state_intial_destination = os.path.join(destination, 'processed')


  # Load vairables 
  divvy_final_destination = os.path.join(destination, 'final')
  output_filename = 'divvy_final.csv'


  extract_divvy_biketrip_dataset(start_date, end_date, divvy_intial_destination, date_format="%Y-%m-%d")
  download_file_from_web(neighborhood_url, neighborhood_filename, neighborhood_intial_destination)
  download_file_from_web(state_url, state_filename, state_intial_destination)

  raw_divvy_df = extract_all_files_in_directory(divvy_intial_destination, 
                                                file_ext=file_ext, 
                                                sub_dir=sub_dir,
                                                dtype=dtype,
                                                parse_dates=parse_dates,
                                                date_parser=date_parser)
  transformed_df = transform_data(raw_divvy_df)
  send_to_csv(transformed_df, output_filename, divvy_final_destination)

if __name__ == "__main__":
  main() # ~ 30 minutes to run