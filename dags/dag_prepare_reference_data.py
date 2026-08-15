import datetime
import pendulum
import pandas as pd
import geopandas as gpd
import io

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag(
    dag_id="dag_prepare_reference_data",
    schedule="@once",
    start_date=pendulum.datetime(2025, 8, 27, tz="America/Sao_Paulo"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

def dag_prepare_reference_data():

    @task
    def prepare_reference_data():

        params = {'key': 'reference_data/kml/coordinates.kml', 
                  'bucket_name': 'juliana-datalake-raw'}
        try:
            hook = S3Hook('aws_conn')
            file_name = hook.download_file(**params)

        except Exception as e:
            print(f"Unable to fetch data from S3: {e}.")

            raise
        else:
            try:
                gdf = gpd.read_file(file_name, engine="pyogrio")

                data = pd.DataFrame({'Name': gdf['Name'].to_list(),
                        'Lat': gdf.geometry.y.to_list(),
                        'Long': gdf.geometry.x.to_list()})

                parquet_bytes = data.to_parquet(index=False)
                parquet_buffer = io.BytesIO(parquet_bytes)

                params = {'file_obj': parquet_buffer, 
                        'key': 'reference_data/coordinates.parquet',
                        'bucket_name': 'juliana-datalake-raw',
                        'replace': True}
                
                hook = S3Hook('aws_conn')
                file_name = hook.load_file_obj(**params)

                path = f"s3://{params['bucket_name']}/{params['key']}"

            except Exception as e:
                print(f"Unable to fetch data from S3: {e}.")
            
                raise
            else:
                return path

    prepare_reference_data()

extracao_dag = dag_prepare_reference_data()