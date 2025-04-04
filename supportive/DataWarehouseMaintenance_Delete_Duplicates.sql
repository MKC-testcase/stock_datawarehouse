DELETE 
FROM StockDataWarehouse.dbo.StockInformation
WHERE StockID NOT IN (
	SELECT MIN(StockID)
	FROM StockDataWarehouse.dbo.StockInformation
	GROUP BY [Date], [TickerSymbol]
)