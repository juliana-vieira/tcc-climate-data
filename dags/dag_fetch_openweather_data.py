import datetime
import pendulum
import pandas as pd
import geopandas as gpd
import io
import json

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook

@dag(
    dag_id="dag_fetch_api_data",
    schedule="35 4 * * *",
    start_date=pendulum.datetime(2025, 8, 27, tz="America/Sao_Paulo"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def dag_fetch_api_data():

    @task
    def fetch_api_data():

        hook = S3Hook('aws_conn')
        file = hook.download_file(
                key='reference_data/coordinates.parquet',
                bucket_name='juliana-datalake-raw'
            )

        df = pd.read_parquet(file)

        http_hook = HttpHook(method="GET", http_conn_id="openweather_conn")
        conn = http_hook.get_connection("openweather_conn")
        key = conn.password

        dados_json = []

        for index, row in df.iterrows():  

            params = {"lat": row['Lat'], "lon": row['Long'], "appid": key, "units": 'metric'}
            response = http_hook.run(endpoint="data/2.5/weather", data=params)

            dados = {
                'nome_cidade' : row['Name'],
                'latitude': row['Lat'],
                'longitude' : row['Long'],
                'dados_meteorologicos' : response.json()
            }

            dados_json.append(dados)

        json_string = json.dumps(dados_json)

        hook.load_string(
                string_data=json_string,
                key='staging/dados-clima.json',
                bucket_name='juliana-datalake-raw',
                replace=True
        )

        file_info = {'key': 'staging/dados-clima.json', 'bucket_name': 'juliana-datalake-raw'}

        return file_info

    @task
    def upload_to_s3(file_info):

        hook = S3Hook('aws_conn')
        file = hook.download_file(**file_info)

        df = pd.read_json(file)

        parquet_bytes = df.to_parquet(index=False)
        parquet_buffer = io.BytesIO(parquet_bytes)

        hook = S3Hook('aws_conn')
        hook.load_file_obj(
            file_obj=parquet_buffer,
            key=f'tcc-climate-data/dados_clima_{datetime.datetime.today()}.parquet',
            bucket_name="juliana-datalake-raw",
            replace=False
            )

    data = fetch_api_data()
    upload_to_s3(data)

extracao_dag = dag_fetch_api_data()
