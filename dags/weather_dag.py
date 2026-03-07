from airflow import DAG
from datetime import datetime, UTC,timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import API_KEY
import os
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}
def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = (temp_in_kelvin - 273.15)
    return temp_in_celsius
def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp = kelvin_to_celsius(data["main"]["temp"])
    feels_like= kelvin_to_celsius(data["main"]["feels_like"])
    min_temp = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    timezone_offset = timedelta(seconds=data['timezone'])
    time_of_record = datetime.fromtimestamp(data['dt'], UTC) + timezone_offset
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'], UTC) + timezone_offset
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'], UTC) + timezone_offset
    transformed_data = {"City": city,
                            "Description": weather_description,
                            "Temperature (C)": temp,
                            "Feels Like (C)": feels_like,
                            "Minimum Temp (C)":min_temp,
                            "Maximum Temp (C)": max_temp,
                            "Pressure": pressure,
                            "Humidity": humidity,
                            "Wind Speed": wind_speed,
                            "Time of Record": time_of_record,
                            "Sunrise (Local Time)":sunrise_time,
                            "Sunset (Local Time)": sunset_time                        
                            }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_hanoi_' + dt_string
    file_path = f"/tmp/{dt_string}.csv"
    
    df_data.to_csv(file_path, index=False)
    return file_path
def load_csv_to_postgres(task_instance):
    csv_file_path = task_instance.xcom_pull(task_ids="transform_load_weather_data")
    df = pd.read_csv(csv_file_path) 

    # Gọi hook của Postgres
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        name='daily_weather_data',
        con=engine,
        schema='public',       
        if_exists='append',
        index=False
    )
    os.remove(csv_file_path)
with DAG(
    dag_id='weather_dag_1',
    default_args=default_args,
    schedule='@daily',   
    catchup=False
) as dag:

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=f'data/2.5/weather?q=Hanoi&appid={API_KEY}'
    )
    extract_weather_data = HttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint=f'data/2.5/weather?q=Hanoi&appid={API_KEY}',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )
    transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )
    load_csv_to_sql_server = PythonOperator(
        task_id='load_csv_to_sql_server',
        python_callable=load_csv_to_postgres
    )
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> load_csv_to_sql_server