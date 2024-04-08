import json
import requests
from datetime import datetime, timedelta

def bulk_json_to_csv(dir_path, file_prefix, from_date, till_date):
        # file_prefix:"xrates_aud_nzd_"):
        # dir_path: "../data/raw/"
        # from_date: "2024-01-01",
        # till_date: "2024-01-30"
        print("from "+from_date)
        print("till "+till_date)
        print(dir_path)
        print(file_prefix)