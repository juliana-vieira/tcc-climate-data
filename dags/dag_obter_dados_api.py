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
    dag_id="dag_obter_dados_api",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 8, 22, tz="America/Sao_Paulo"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def dag_obter_dados_api():

    @task
    def extracao():

        hook = S3Hook('aws_conn')
        arquivo = hook.download_file(
                key='reference_data/coordinates.json',
                bucket_name='juliana-datalake-raw'
            )

        with open(arquivo, 'r', encoding='utf-8') as f:
            dados_referencia = json.loads(f.read())

        http_hook = HttpHook(method="GET", http_conn_id="openweather_conn")
        conn = http_hook.get_connection("openweather_conn")
        key = conn.password

        dados_lista = []

        for bairro in dados_referencia:

            params = {"lat": bairro['Lat'], "lon": bairro['Long'], "appid": key, "units": "metric", "lang": 'pt_br'}
            response = http_hook.run(endpoint="data/2.5/weather", data=params)

            dados = {
                'id': bairro['id'],
                'nome_bairro' : bairro['Name'],
                'latitude': bairro['Lat'],
                'longitude' : bairro['Long'],
                'dados_meteorologicos' : json.dumps(response.json())
            }

            dados_lista.append(dados)

        json_string = json.dumps(dados_lista, default=str, ensure_ascii=False)

        hook.load_string(
                string_data=json_string,
                key='staging/dados-clima.json',
                bucket_name='juliana-datalake-raw',
                replace=True
        )

        staging_key = 'staging/dados-clima.json'

        return staging_key

    @task
    def upload_s3(staging_key):

        hook = S3Hook('aws_conn')

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        final_key = f'tcc-climate-data/dados_clima_{timestamp}.json'
        bucket = 'juliana-datalake-raw'

        hook.copy_object(
            source_bucket_key=staging_key,
            dest_bucket_key=final_key,
            source_bucket_name=bucket,
            dest_bucket_name=bucket
        )

    data = extracao()
    upload_s3(data)

extracao_dag = dag_obter_dados_api()
