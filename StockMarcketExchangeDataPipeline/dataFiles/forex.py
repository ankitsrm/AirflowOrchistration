import csv
import requests
import json


def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('D:\\Airflow\\dockerCompose\\mnt\\airflow\\dags\\files\\forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            print(idx)
            print(row)
            print("Reader : ", reader)            
            base = row['base']
            print("Base : ", base)
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            print("Indata : ",indata)
            print("Outdata : ",outdata)
            for pair in with_pairs:
                print("*******")
                print("With Pairs : ", with_pairs)
                print("Pair : ", pair)
               ## print("outdata['rates'][pair] : ", outdata['rates'][pair])
                print("indata['rates'][pair] : ",indata['rates'][pair])
                
                outdata['rates'][pair] = indata['rates'][pair]
                print("*******")
            with open('forex_rates.json', 'a') as outfile:
                print ("outdata : {}, outfile : {}", outdata,outfile)
                json.dump(outdata, outfile)
                outfile.write('\n')
                
                
download_rates()