


WITH temp AS (
SELECT Symbol
     , purchase_date = t.Trade_Date
     , expiration_date = t.ExpirationDate
     , strike = t.StrikePrice
     , qty
	, Notional = t.Qty * t.StrikePrice * 100
	, category = CASE WHEN t.Qty > 0 THEN 'long' ELSE 'short' END
FROM hdg.tblOptionTradingActivity t
WHERE t.TransactionType = 'TRADES'

EXCEPT

SELECT Symbol
     , purchase_date
     , expiration_date
     , strike
     , qty
     , Notional
     , category
FROM hdg.tblHedgingOptionsTrend
)

INSERT INTO hdg.tblHedgingOptionsTrend (
       Symbol
     , purchase_date
     , expiration_date
     , strike
     , qty
     , Notional
     , category
)
SELECT temp.Symbol
     , temp.purchase_date
     , temp.expiration_date
     , temp.strike
     , temp.Qty
     , temp.Notional
     , temp.category
FROM temp