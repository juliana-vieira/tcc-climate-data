import datetime
import pendulum
import pandas as pd
import geopandas as gpd
import uuid
import io

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag(
    dag_id="dag_preparar_dados_referencia",
    schedule="@once",
    start_date=pendulum.datetime(2025, 8, 27, tz="America/Sao_Paulo"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

def dag_preparar_dados_referencia():

    @task
    def preparar_dados():

        params = {'key': 'reference_data/kml/coordinates.kml', 
                  'bucket_name': 'juliana-datalake-raw'}
        try:
            hook = S3Hook('aws_conn')
            arquivo = hook.download_file(**params)

        except Exception as e:
            print(f"Erro ao obter dados do S3 {e}.")

            raise
        else:
            try:
                gdf = gpd.read_file(arquivo, engine="pyogrio")

                gdf['Lat'] = gdf.geometry.y
                gdf['Long'] = gdf.geometry.x
                gdf['id'] = [str(uuid.uuid4()) for _ in range(len(gdf))]

                json_string = gdf[['id', 'Name', 'Lat', 'Long']].to_json(orient='records', force_ascii=False)

                params = {'string_data': json_string, 
                        'key': 'reference_data/coordinates.json',
                        'bucket_name': 'juliana-datalake-raw',
                        'replace': True}
                
                hook = S3Hook('aws_conn')
                arquivo = hook.load_string(**params)

                path = f"s3://{params['bucket_name']}/{params['key']}"

            except Exception as e:
                print(f"Erro ao fazer upload pro S3: {e}.")
            
                raise
            else:
                return path

    preparar_dados()

extracao_dag = dag_preparar_dados_referencia()