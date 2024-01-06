#!/bin/bash

airflow tasks test stock_data_pipeline get_stock_rates 2021-01-01
airflow tasks test stock_data_pipeline saving_rates 2021-01-01
airflow tasks test stock_data_pipeline createStockRatesTable 2021-01-01
airflow tasks test stock_data_pipeline stockProcessing 2021-01-01
