import csv
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import bulk_json_to_csv as bconv
import duckdb

#url = "https://api.freecurrencyapi.com/v1/historical?"
#url = "https://api.currencybeacon.com/v1/historical"
url = "https://api.currencybeacon.com/v1/timeseries?"
payload = {}
# headers = {
#   'Content-Type': 'application/json',
#   'apikey': 'fca_live_wV6DmZjVMNWI4vtlHCnW4eCD9XlVMvkDM1cElOSb'
# }

headers = {
  'Content-Type': 'application/json'
}

def get_xrates(date, base_currency, target_currency):
    parameters = {
        "date":date,
        "base_currency":base_currency,
        "currencies":target_currency,
        'api_key': 'rgdJ79Dc2CIhtc7ktQol6SvloWJfnrfB'
    }
    response = requests.request("GET", url, params=parameters, headers=headers, data=payload)
    return response.json() 

def get_xrates(start_date, end_date, base_currency, target_currency):
    parameters = {
        "start_date":start_date,
        "end_date":end_date,
        "base":base_currency,
        "base":target_currency
    }
    response = requests.request("GET", url, params=parameters, headers=headers, data=payload)
    return response.json()

def get_xrates_series(start_date, end_date, base_currency, target_currency):
    parameters = {
        "start_date":start_date,
        "end_date":end_date,
        "base":base_currency,
        "symbols":target_currency,
        'api_key': 'rgdJ79Dc2CIhtc7ktQol6SvloWJfnrfB'
    }
    response = requests.request("GET", url, params=parameters, headers=headers, data=payload)
    return response.json()

def get_xrates_in_range(start_date, end_date, base_curr, target_curr):
    for i in range((end_date - start_date).days):
        current_date = start_date + timedelta(days=i)
        now = datetime.now()
        timestamp_str = now.strftime("%Y%m%d%H%M%S")
        filename = "../data/raw/xrates_"+base_curr+"_"+target_curr+"_"+current_date.strftime("%Y%m%d")+"_"+str(timestamp_str)+".json"
        with open(filename.lower(), "w") as f:
            f.write(json.dumps(get_xrates(str(current_date)[0:10], base_curr, target_curr)))



def load_csv_duck_db_table(sqlfile):
    #duckdb.read_csv("..\data\staging\xrates_aud_nzd_20240308_20240407_20240407165247.csv")                # read a CSV file into a Relation
    con = duckdb.connect()

    # Read the SQL query from the file
    with open(sqlfile, "r") as f:
        query = f.read()
    con.execute(query)

    # Get the results
    results = con.fetchall()

    # Print the results
    for row in results:
        print(row)

    # Close the connection
    con.close()

    # ss=duckdb.sql("SELECT * FROM '"+sqlfile+"'")     # directly query a CSV file
    #print(ss)

def get_xrates_date_series(start_date, end_date, base_curr, target_curr):
        now = datetime.now()
        timestamp_str = now.strftime("%Y%m%d%H%M%S")
        filename="xrates_"+base_curr+"_"+target_curr+"_"+start_date.strftime("%Y%m%d")+"_"+end_date.strftime("%Y%m%d")+"_"+str(timestamp_str)
        abs_filename = ("../data/raw/"+filename+".json").lower()
        with open(abs_filename, "w") as f:
            f.write(json.dumps(get_xrates_series(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), base_curr, target_curr)))
        
        with open(abs_filename) as train_file:
            dict_train = json.load(train_file)
        rates_df = pd.DataFrame.from_dict(dict_train["response"], orient='index')
        #print(rates_df)

        # Add custom column with default value        
        rates_df['BaseCurrency'] = "AUD"
        rates_df['TargetCurrency'] = "NZD"
        rates_df['Filename'] = abs_filename
        rates_df['LoadDate'] = now.strftime("%Y-%m-%d %H:%M:%S")
        
        processed_dir = "..\\data\\staging\\"
        abs_processed_filename = processed_dir+filename+".csv"

        rates_df.reset_index(inplace=True)

        # Rename the index column if desired
        rates_df.rename(columns={'index': 'TradingDate', 'NZD':'CurrencyRate'}, inplace=True)
        rates_df.to_csv(abs_processed_filename.lower(), index=False) #, quoting=csv.QUOTE_ALL)
        #load_csv_duck_db_table(abs_processed_filename.lower()) 
        result = rates_df.groupby(['BaseCurrency','TargetCurrency']).aggregate({'CurrencyRate':['min','max','mean','sum']})
        print(result)
        #print(result[min][0])
        print(rates_df[(rates_df["CurrencyRate"] == result[min][0]) | (rates_df["CurrencyRate"] == result[max][0])].to_csv(".\\abc.csv"))
        
        #BaseCurrency
        # , TargetCurrency
        # , MAX(CurrencyRate) MaxCurrRate
        # , MIN(CurrencyRate) MinCurrRate
        # , AVG(CurrencyRate) AvgCurrRate
        # , SUM(CurrencyRate) SumCurrRate


        #load_csv_duck_db_table("..\\sql\\xrates_analysis.sql") 
end_date  = datetime.now()
start_date = end_date - timedelta(days=30)
base_currency = "AUD"
target_currency = "NZD"
#get_xrates_in_range(start_date, end_date, base_currency, target_currency)
get_xrates_date_series(start_date, end_date, base_currency, target_currency)




# dir_path="../data/raw/"
# file_prefix=("xrates_"+base_currency+"_"+target_currency+"_").lower()
# bconv.bulk_json_to_csv(dir_path, file_prefix, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
#         # file_prefix:"xrates_aud_nzd_"):
#         # dir_path: "../data/raw/"
#         # from_date: "2024-01-01",
#         # till_date: "2024-01-30"


