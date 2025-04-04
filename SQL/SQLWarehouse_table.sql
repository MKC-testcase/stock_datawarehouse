USE StockDataWarehouse;
GO

CREATE TABLE StockInformation (
    StockID INT PRIMARY KEY IDENTITY(1,1),
    TickerSymbol NVARCHAR(10),
    Date DATE,
    OpenPrice FLOAT,
    ClosePrice FLOAT,
    HighPrice FLOAT,
    LowPrice FLOAT,
    Volume BIGINT
);