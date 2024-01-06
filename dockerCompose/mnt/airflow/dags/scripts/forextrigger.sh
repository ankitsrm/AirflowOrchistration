#!/bin/bash

airflow tasks test forex_data_pipeline is_forex_rates_available 2021-01-01
airflow tasks test forex_data_pipeline is_forex_currencies_file_available 2021-01-01
airflow tasks test forex_data_pipeline downloading_rates 2021-01-01
airflow tasks test forex_data_pipeline saving_rates 2021-01-01
airflow tasks test forex_data_pipeline creating_forex_rates_table 2021-01-01
airflow tasks test forex_data_pipeline forex_processing 2021-01-01
