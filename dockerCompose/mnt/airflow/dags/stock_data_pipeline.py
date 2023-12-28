from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

import csv
import requests
import json
import os

default_args = {
    "owner":"ankit",
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"admin@localhost.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5)

}

def getMatchedStockPrice():
    
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/stock_currencies.csv') as stock_rates:
        reader=csv.DictReader(stock_rates, delimiter=';')
        current_direct="/opt/airflow/dags/files"
        timestamp= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
            
        filename=f"{current_direct}/output/stock_rates_{timestamp}.json"
        print(filename)
        os.makedirs(os.path.dirname(filename),exist_ok=True)
        for idx,row in enumerate(reader):
            base=row['base']
            withpairs=row['with_pairs'].split(' ')
            indata=requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata={'base':base,'rates':{},'last_update':indata['date']}
            for pair in withpairs:
                outdata['rates'][pair]=indata['rates'][pair]          
            
            with open(filename,'a')as outfile:
                json.dump(outdata,outfile)
                outfile.write('\n')
                
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
    
    is_stock_file_available = FileSensor(
        task_id="is_stock_file_available",
        fs_conn_id="stock_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20    
        )
    
    is_stock_rates_available=PythonOperator(
        task_id="get_stock_rates",
        python_callable=getMatchedStockPrice
    )
    
    saving_rates=BashOperator(
        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /stock && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/output/stock* /stock &&\
            rm -r $AIRFLOW_HOME/dags/files/output/stock*
        """
    )

    creat_stock_rates_table=HiveOperator(
        task_id="createStockRatesTable",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS stock_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
            )
            
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
        
    )
