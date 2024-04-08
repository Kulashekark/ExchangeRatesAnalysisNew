import json
import requests
from datetime import datetime, timedelta

url = "https://api.freecurrencyapi.com/v1/historical?"

payload = {}
headers = {
  'Content-Type': 'application/json',
  'apikey': 'fca_live_wV6DmZjVMNWI4vtlHCnW4eCD9XlVMvkDM1cElOSb'
}

def get_xrates(date, base_currency, target_currency):
    parameters = {
        "date":date,
        "base_currency":base_currency,
        "currencies":target_currency
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
end_date  = datetime.now()
#print (start_date)
start_date    = end_date - timedelta(days=5)
#print (end_date)
base_currency = "AUD"
target_currency = "NZD"
get_xrates_in_range(  start_date, end_date, base_currency, target_currency)

