import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url0 = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    files_list = []
    files_list.append(f"{url0}/green_tripdata_2022-01.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-02.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-03.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-04.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-05.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-06.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-07.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-08.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-09.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-10.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-11.parquet")
    files_list.append(f"{url0}/green_tripdata_2022-12.parquet")
    # url0 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green'
    # files_list = []
    # files_list.append(f"{url0}/green_tripdata_2020-10.csv.gz")
    # files_list.append(f"{url0}/green_tripdata_2020-11.csv.gz")
    # files_list.append(f"{url0}/green_tripdata_2020-12.csv.gz")

    df_list = []
    
    # taxi_dtypes = {
    #     'VendorID': pd.Int64Dtype(),
    #     'store_and_fwd_flag': str,
    #     'RatecodeID': pd.Int64Dtype(),
    #     'PULocationID': pd.Int64Dtype(),
    #     'DOLocationID': pd.Int64Dtype(),
    #     'passenger_count': pd.Int64Dtype(),
    #     'trip_distance': float,    
    #     'fare_amount': float,
    #     'extra': float,
    #     'mta_tax': float,
    #     'tip_amount': float,
    #     'tolls_amount': float,
    #     'ehai fee': float,
    #     'improvement_surcharge': float,
    #     'total_amount': float,
    #     'payment_type': pd.Int64Dtype(), 
    #     'trip_type': pd.Int64Dtype(),   
    #     'congestion_surcharge': float 
    # }
    # parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    # Loop through the list of CSV files
    for file in files_list:
        # Read each CSV file into a DataFrame
        df=pd.read_parquet(file)
        #df=pd.read_csv(file, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        # Append the DataFrame to the list
        df_list.append(df)
    
    # Concatenate all the DataFrames in the list
    final_df = pd.concat(df_list, ignore_index=True)
    
    return final_df
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
