SELECT
 BaseCurrency
, TargetCurrency
, MAX(CurrencyRate) MaxCurrRate
, MIN(CurrencyRate) MinCurrRate
, AVG(CurrencyRate) AvgCurrRate
, SUM(CurrencyRate) SumCurrRate
FROM '..\\data\\staging\\xrates_aud_nzd_20240308_20240407_20240407171859.csv'
GROUP BY BaseCurrency,TargetCurrency
