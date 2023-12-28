import requests

BASE_URL="https://github.com/ankitsrm/AirflowOrchistration/tree/main/StockMarcketExchangeDataPipeline/dataFiles"

indata = requests.get(BASE_URL).json()
print(indata)