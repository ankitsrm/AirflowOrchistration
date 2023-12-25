from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor

from datetime import datetime,timedelta

default_args = {
    "owner":"ankit",
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"admin@localhost.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5)

}
with DAG("stock_data_pipeline",start_date=datetime(2021,1,1),
         schedule_interval="@daily",default_args=default_args) as dag: 
    
    is_stock_rates_available = HttpSensor(
        task_id="is_stock_rates_available",#it has to be unique in the same dag
        http_conn_id="stock_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20,
    )
