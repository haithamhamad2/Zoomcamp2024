import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/sonic-airfoil-411903-6574d9065f21.json"

bucket_name = "mage-zoomcamp-sonic-airfoil-411903"
project_id = "sonic-airfoil-411903"
table_name = "green_taxi_data"
root_path = f'{bucket_name}/{table_name}'
@data_exporter
def export_data(data, *args, **kwargs):
    # create date colums from the datetime columns
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # define pyarrow table
    table = pa.Table.from_pandas(data)

    # define bucket google, it authorizes fs automatically
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )

