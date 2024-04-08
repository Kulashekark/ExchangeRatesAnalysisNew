import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import bulk_json_to_csv as bconv

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

# def get_xrates(date, base_currency, target_currency):
#     parameters = {
#         "date":date,
#         "base_currency":base_currency,
#         "currencies":target_currency,
#         'api_key': 'rgdJ79Dc2CIhtc7ktQol6SvloWJfnrfB'
#     }
#     response = requests.request("GET", url, params=parameters, headers=headers, data=payload)
#     return response.json() 

# def get_xrates(start_date, end_date, base_currency, target_currency):
#     parameters = {
#         "start_date":start_date,
#         "end_date":end_date,
#         "base":base_currency,
#         "base":target_currency
#     }
#     response = requests.request("GET", url, params=parameters, headers=headers, data=payload)
#     return response.json()

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

# def get_xrates_in_range(start_date, end_date, base_curr, target_curr):
#     for i in range((end_date - start_date).days):
#         current_date = start_date + timedelta(days=i)
#         now = datetime.now()
#         timestamp_str = now.strftime("%Y%m%d%H%M%S")
#         filename = "../data/raw/xrates_"+base_curr+"_"+target_curr+"_"+current_date.strftime("%Y%m%d")+"_"+str(timestamp_str)+".json"
#         with open(filename.lower(), "w") as f:
#             f.write(json.dumps(get_xrates(str(current_date)[0:10], base_curr, target_curr)))


def get_xrates_date_series(start_date, end_date, base_curr, target_curr):
        now = datetime.now()
        timestamp_str = now.strftime("%Y%m%d%H%M%S")
        filename = "../data/raw/xrates_"+base_curr+"_"+target_curr+"_"+start_date.strftime("%Y%m%d")+"_"+end_date.strftime("%Y%m%d")+"_"+str(timestamp_str)+".json"
        with open(filename.lower(), "w") as f:
            f.write(json.dumps(get_xrates_series(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), base_curr, target_curr)))
        
        with open(filename) as train_file:
            dict_train = json.load(train_file)
        train = pd.DataFrame.from_dict(dict_train["response"], orient='index')
        print(train)

end_date  = datetime.now()
start_date = end_date - timedelta(days=30)
base_currency = "AUD"
target_currency = "NZD"
#get_xrates_in_range(start_date, end_date, base_currency, target_currency)
get_xrates_date_series(start_date, end_date, base_currency, target_currency)


dir_path="../data/raw/"
file_prefix=("xrates_"+base_currency+"_"+target_currency+"_").lower()
bconv.bulk_json_to_csv(dir_path, file_prefix, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        # file_prefix:"xrates_aud_nzd_"):
        # dir_path: "../data/raw/"
        # from_date: "2024-01-01",
        # till_date: "2024-01-30"
