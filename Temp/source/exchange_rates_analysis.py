import csv
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import bulk_json_to_csv as bconv
import duckdb

url = "https://api.currencybeacon.com/v1/timeseries?"
payload = {}

headers = {
  'Content-Type': 'application/json'
}

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

def execute_sql_file_duckdb(sqlfile):
    con = duckdb.connect()
    with open(sqlfile, "r") as f:
        query = f.read()
    con.execute(query)
    results = con.fetchall()
    print(results)
    con.close()

def get_xrates_date_series(start_date, end_date, base_curr, target_curr):
    now = datetime.now()
    timestamp_str = now.strftime("%Y%m%d%H%M%S")
    filename="xrates_"+base_curr+"_"+target_curr+"_"+start_date.strftime("%Y%m%d")+"_"+end_date.strftime("%Y%m%d")+"_"+str(timestamp_str)
    abs_filename = ("../data/raw/"+filename+".json").lower()
    with open(abs_filename, "w") as f:
        f.write(json.dumps(get_xrates_series(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), base_curr, target_curr)))
    
    res_dict = {}
    res_dict["json_filename"] = filename
    res_dict["abs_filename"] = abs_filename
    res_dict["LoadDate"] = now.strftime("%Y-%m-%d %H:%M:%S")
    res_dict["BaseCurrency"] = base_curr
    res_dict["TargetCurrency"] = target_curr

    return res_dict


def json_to_csv(j_dict):
    #json to CSV
    with open(j_dict["abs_filename"]) as json_file:
        dict_json = json.load(json_file)
    rates_df = pd.DataFrame.from_dict(dict_json["response"], orient='index')

    rates_df['BaseCurrency'] = j_dict["BaseCurrency"]
    rates_df['TargetCurrency'] = j_dict["TargetCurrency"]
    rates_df['Filename'] = j_dict["json_filename"]
    rates_df['LoadDate'] = j_dict["LoadDate"]

    processed_dir = "..\\data\\staging\\"
    abs_processed_filename = processed_dir+j_dict["json_filename"]+".csv"

    rates_df.reset_index(inplace=True)

    # Rename the index column if desired
    rates_df.rename(columns={'index': 'TradingDate', 'NZD':'CurrencyRate'}, inplace=True)
    rates_df.to_csv(abs_processed_filename.lower(), index=False) #, quoting=csv.QUOTE_ALL)
    
    execute_sql_file_duckdb("..\\sql\\xrates_analysis.sql")
    #load_csv_duck_db_table("..\\sql\\xrates_analysis.sql")
    
end_date  = datetime.now()
start_date = end_date - timedelta(days=30)
base_currency = "AUD"
target_currency = "NZD"
#get_xrates_in_range(start_date, end_date, base_currency, target_currency)

json_dict = get_xrates_date_series(start_date, end_date, base_currency, target_currency)
json_to_csv(json_dict)
string_dict  = {}
string_dict["$str_0$"]="..\\data\\staging\\xrates_aud_nzd_20240308_20240407_20240407201005.csv"
#substitute_strings_in_file("..\\sql\\xrates_analysis.sql", string_dict)