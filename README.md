# ExchangeRatesAnalysis
 
Challenge:
•	Connect to any exchange rates API (Sample - Exchange Rates API) to get exchange rates of Australia to New Zealand for the past 30 days into json output format
•	Pre process the data to manage any expected issues
•	Perform some data analysis
o	Find the best and worst exchange rates for that time period
o	Calculate the average exchange rate for the month

Important Code Snippets:
API Used: 
url = "https://api.currencybeacon.com/v1/timeseries?"
exchange_rates_analysis_before_final.py
xrates_analysis.sql

Sample code snippets to download the JSON file using an API are provided. I tired to use the Python, DataFrames, DuckDB to process the required ExchangeRates info.

Possible test cases are provided on a high level in a excel file.

Analysed output:
The output gives the Maximum Currency Rate, Maximum Currency Trading Date,  Minimum Currency Rate, Minimum Currency Trading Date, Average Currency Rate, Percentage changed based on the MINIMUM Currency rate.

[('BaseCurrency', 'TargetCurrency', 'MaxCurrRate', 'MaxTradeDate', 'MinCurrRate', 'MinTradeDate', 'AvgCurrRate', 'PercentageCurrRate'),
 ('AUD', 'NZD', '1.09443405', '2024-04-05 00:00:00', '1.07123743', '2024-03-10 00:00:00', '1.0844122506451614', '3.2556183474084137')]
