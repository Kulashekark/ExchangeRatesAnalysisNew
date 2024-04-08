Select
BaseCurrency, TargetCurrency, MaxCurrRate, MAX(MaxTradeDate) Max_TradeDate, MinCurrRate, MAX(MinTradeDate) Min_TradeDate , AvgCurrRate, PercentageCurrRate
from
(
Select 
Y.BaseCurrency, Y.TargetCurrency, X.MaxCurrRate, 
CASE WHEN X.MaxCurrRate = Y.CurrencyRate THEN MAX(Y.TradingDate) else strptime('1990-01-01', '%Y-%m-%d') end MaxTradeDate,
CASE WHEN X.MinCurrRate = Y.CurrencyRate THEN MIN(Y.TradingDate) else strptime('1990-01-01', '%Y-%m-%d') end MinTradeDate,
X.MinCurrRate, X.AvgCurrRate, X.PercentageCurrRate
from read_csv('..\data\staging\xrates_aud_nzd_20240109_20240408_20240408230355.csv', dateformat = '%Y-%m-%d') Y
join
(SELECT
 BaseCurrency
, TargetCurrency
, MAX(CurrencyRate) MaxCurrRate
, MIN(CurrencyRate) MinCurrRate
, AVG(CurrencyRate) AvgCurrRate
, (MAX(CurrencyRate)/SUM(CurrencyRate))*100 PercentageCurrRate
FROM read_csv('..\data\staging\xrates_aud_nzd_20240109_20240408_20240408230355.csv', dateformat = '%Y-%m-%d')
GROUP BY BaseCurrency,TargetCurrency
) X on Y.CurrencyRate in (X.MinCurrRate, X.MaxCurrRate)
Group by Y.BaseCurrency, Y.TargetCurrency, X.MaxCurrRate, X.MinCurrRate, X.AvgCurrRate, X.PercentageCurrRate, Y.CurrencyRate
) Z
Group by BaseCurrency, TargetCurrency, MaxCurrRate, MinCurrRate, AvgCurrRate, PercentageCurrRate