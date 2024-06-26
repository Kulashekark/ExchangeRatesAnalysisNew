import csv
import json
import shutil
import requests
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import matplotlib.pyplot as plt

url = "https://api.currencybeacon.com/v1/timeseries?"
payload = {}

headers = {
  'Content-Type': 'application/json'
}
# This method is used to fetch the exchange rates data from the API with in a DateRange 
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

# This method executes the SQL file and returns the result using the Duckdb 
def execute_sql_file_duckdb(sqlfile):
    with open(sqlfile, "r") as f:
        query = f.read()
    results=duckdb.sql(query)
    return results

# Method is captures the JSON results from Rest API and writes to a JSON file into the raw layer
def get_xrates_date_series(start_date, end_date, base_curr, target_curr):
    now = datetime.now()
    timestamp_str = now.strftime("%Y%m%d%H%M%S")
    filename="xrates_"+base_curr+"_"+target_curr+"_"+start_date.strftime("%Y%m%d")+"_"+end_date.strftime("%Y%m%d")+"_"+str(timestamp_str)
    abs_filename = ("../data/raw/"+filename+".json").lower()
    with open(abs_filename, "w") as f:
        f.write(json.dumps(get_xrates_series(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), base_curr, target_curr)))
    
    # Returning the created JSON file and request attributes    
    res_dict = {}
    res_dict["json_filename"] = filename
    res_dict["abs_filename"] = abs_filename
    res_dict["LoadDate"] = now.strftime("%Y-%m-%d %H:%M:%S")
    res_dict["BaseCurrency"] = base_curr
    res_dict["TargetCurrency"] = target_curr
    return res_dict

# This method is used to substitue the filenames in the SQL file with the latest paramters generated CSV filename
def substitute_strings_in_file(filepath, string_dict):
    # substitute_strings_in_file
    with open(filepath, "r+") as f:
        contents = f.read()
        for key in string_dict.keys():
            contents = contents.replace(key, string_dict[key])
        f.seek(0)
        f.write(contents)
        f.truncate()  

# This method converts JSON file into CSV file with the required columns using the Dataframes
def json_to_csv(j_dict):
    #json to CSV
    with open(j_dict["abs_filename"]) as json_file:
        dict_json = json.load(json_file)
    rates_df = pd.DataFrame.from_dict(dict_json["response"], orient='index')

    rates_df['BaseCurrency'] = j_dict["BaseCurrency"]
    rates_df['TargetCurrency'] = j_dict["TargetCurrency"]
    rates_df['Filename'] = j_dict["abs_filename"]
    rates_df['LoadDate'] = j_dict["LoadDate"]

    processed_dir = "..\\data\\staging\\"
    abs_processed_filename = processed_dir+j_dict["json_filename"]+".csv"

    rates_df.reset_index(inplace=True)

    # Rename the index column if desired
    rates_df.rename(columns={'index': 'TradingDate', 'NZD':'CurrencyRate'}, inplace=True)
    rates_df.to_csv(abs_processed_filename.lower(), index=False, quoting=csv.QUOTE_ALL)
    return abs_processed_filename.lower()
    #load_csv_duck_db_table("..\\sql\\xrates_analysis.sql")

# Currently configured as StartDate = CurrentDate and EndDate = CurrentDate - 90 Days    
end_date  = datetime.now()
start_date = end_date - timedelta(days=90)
base_currency = "AUD"
target_currency = "NZD"

# This is used to capture the JSON and request attributes
json_dict = get_xrates_date_series(start_date, end_date, base_currency, target_currency)

# This is used to convert JSON to CSV file and captures the created CSV info
csv_file = json_to_csv(json_dict)
string_dict  = {}
string_dict["$str_0$"]=csv_file
sql_file = "xrates_analysis.sql"
shutil.copy2('..\\sql\\'+sql_file, '..\\temp\\')
substitute_strings_in_file("..\\temp\\"+sql_file, string_dict)
res_list = execute_sql_file_duckdb("..\\temp\\"+sql_file)

print(res_list)

# This is used to generate the Line graph for the provided date range
gp_df=duckdb.sql("SELECT TradingDate, CurrencyRate FROM '"+csv_file+"'").fetchdf()
gp_df.plot(x='TradingDate', y='CurrencyRate', kind='line')
plt.savefig("..\\graphs\\"+json_dict["json_filename"].lower()+".png")
plt.show()
