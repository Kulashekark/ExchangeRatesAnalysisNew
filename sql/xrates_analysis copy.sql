DECLARE  @startdate DATE = '2024-03-08' -- Start Date
, @enddate DATE = '2024-04-07' -- End Date

-- Gathering the Max, Min & Avg exchange rates between a Date range in the CTE
;with cte as
(
SELECT
 BaseCurrency
, TargetCurrency
, MAX(CurrencyRate) MaxCurrRate
, MIN(CurrencyRate) MinCurrRate
, AVG(CurrencyRate) AvgCurrRate
, SUM(CurrencyRate) SumCurrRate
FROM '..\\data\\staging\\xrates_aud_nzd_20240308_20240407_20240407171859.csv'
WHERE TradingDate BETWEEN @startdate AND @enddate
GROUP BY BaseCurrency,TargetCurrency
)

-- Outer Query to get all the required output info in a Single Row
SELECT DISTINCT ABC.BaseCurrency
, ABC.TargetCurrency
, ABC.MaxCurrRate
, MAX(ABC.MaxTradeDate) MaxTradeDate
, ABC.MinCurrRate
, MAX(ABC.MinTradeDate) MinTradeDate
, AvgCurrRate
, PercentageChange
FROM ( -- Gathering the Min & Max Dates for the currencies
SELECT  A.BaseCurrency
,A.TargetCurrency
,A.MaxCurrRate
,COALESCE(CASE WHEN A.MaxCurrRate = ER.CurrencyRate THEN MAX(ER.TradingDate) END,'') MaxTradeDate
,A.MinCurrRate
,COALESCE(CASE WHEN A.MinCurrRate = ER.CurrencyRate THEN MIN(ER.TradingDate) END,'') MinTradeDate
,A.AvgCurrRate
,(A.MaxCurrRate / A.SumCurrRate) * 100 AS PercentageChange
FROM '..\\data\\staging\\xrates_aud_nzd_20240308_20240407_20240407171859.csv' ER
INNER JOIN
cte A
ON (ER.CurrencyRate = A.MaxCurrRate
OR ER.CurrencyRate = A.MinCurrRate)
WHERE ER.TradingDate BETWEEN @startdate AND @enddate
GROUP BY A.BaseCurrency,A.TargetCurrency,A.MaxCurrRate,A.MinCurrRate,AvgCurrRate,A.SumCurrRate, ER.CurrencyRate
) AS ABC
GROUP BY ABC.BaseCurrency,ABC.TargetCurrency,ABC.MaxCurrRate,ABC.MinCurrRate,  AvgCurrRate,PercentageChange