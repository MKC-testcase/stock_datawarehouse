import pyodbc
import yfinance as yf
import pandas as pd
import os
import datetime
import configparser
import requests

class DW_Stock:
    def __init__(self):
        """
        Initializes configuration parameters and sets up several standard SQL queries used in the class
        """
        config = configparser.ConfigParser()
        config.read("configs\config.ini")
        # setting up the connection to the data warehouse
        # if there is a time where the sql connection lags with no error there maybe an issue with the drivers being updated
        # without your knowledge
        self.conn = pyodbc.connect(driver='{SQL Server Native Client 11.0}',
                              server=config["Data Warehouse"]["server"],
                              database=config["Data Warehouse"]["database"],
                              uid=config["Data Warehouse"]["username"],
                              pwd=config["Data Warehouse"]["password"])
        # initialzing a cursor for general database use
        self.cursor = self.conn.cursor()
        # adding the Alpha Vantage API Key
        self.alpha_vantage_key = config["Alpha Vantage"]["api_key"]
        
        # standard SQL queries
        self.stock_day_check = """ 
            SELECT *
            FROM [StockDataWarehouse].dbo.StockInformation s
            WHERE s.TickerSymbol= ? AND s.Date = ?
        """
        self.insert_stock = """
            INSERT INTO [StockDataWarehouse].dbo.StockInformation (TickerSymbol, Date, OpenPrice, ClosePrice, HighPrice, LowPrice, Volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        self.update_stock = """
            UPDATE [StockDataWarehouse].dbo.StockInformation
            SET OpenPrice = ?, ClosePrice = ?, HighPrice = ?, LowPrice = ?, Volume = ?
            WHERE TickerSymbol = ? AND Date = ?
        """


    def dw_setup(self, ticker):
        """
        Set up new stocks into the database table
        """
        # Alpha Vantage API key
        API_KEY = self.alpha_vantage_key
        # Ticker Abbreviation
        symbol = f'{ticker}'  

        # Get data from Alpha Vantage API
        url = f'https://www.alphavantage.co/query'
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': API_KEY,
            'outputsize': 'full'
        }

        response = requests.get(url, params=params)
        data = response.json()
        if data == []:
            return False

        # Convert data into a DataFrame
        time_series = data['Time Series (Daily)']
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df['Date'] = pd.to_datetime(df.index)
        df['OpenPrice'] = pd.to_numeric(df['1. open'])
        df['ClosePrice'] = pd.to_numeric(df['4. close'])
        df['HighPrice'] = pd.to_numeric(df['2. high'])
        df['LowPrice'] = pd.to_numeric(df['3. low'])
        df['Volume'] = pd.to_numeric(df['5. volume'])

        df = df[['Date', 'OpenPrice', 'ClosePrice', 'HighPrice', 'LowPrice', 'Volume']]

        # Insert the data into SQL Server
        for index, row in df.iterrows():
            self.cursor.execute(self.insert_stock, symbol, row['Date'], row['OpenPrice'], row['ClosePrice'], row['HighPrice'], row['LowPrice'], row['Volume'])

        self.conn.commit()

        # check if the stock is previous available the stock list
        with open(f"{curr_dir}\supportive\stock_list.txt",'r') as file:
            stock_list = file.readlines()
        for ticker in stock_list:
            if ticker.strip() == ticker:
                return True

        # add ticker to list of stocks in warehouse
        curr_dir = os.getcwd()
        with open(f"{curr_dir}\supportive\stock_list.txt",'a') as file:
            file.write(f"{ticker}\n")

        return True


    def dw_update(self):
        """
        Procedure to update stocks within the database
        """
        # Getting list of stocks in database
        curr_dir = os.getcwd()
        with open(f"{curr_dir}\supportive\stock_list.txt",'r') as file:
            stock_list = file.readlines()

        for ticker in stock_list:
            # Running the update for each stock in stock list
            # standardizing the ticker input and removing excess whitespace
            ticker = ticker.strip()

            # exception incase the day is sunday or monday
            if datetime.datetime.today().weekday() == 0 or datetime.datetime.today().weekday() == 7:
                print("There is no stocks today, cancelling today's update")
                return
            
            # download stock information of given ticker for the past day
            stock_data = yf.download(ticker, start =datetime.date.today() - datetime.timedelta(days=1), end=datetime.date.today(), auto_adjust=True)
            stock_data['Date'] = pd.to_datetime(stock_data.index[0])
            stock_data['TickerSymbol'] = ticker

            # Converting the types as need for SQL server types
            stock_data['Open'] = stock_data['Open'].astype(float)
            stock_data['Close'] = stock_data['Close'].astype(float)
            stock_data['High'] = stock_data['High'].astype(float)
            stock_data['Low'] = stock_data['Low'].astype(float)
            stock_data['Volume'] = stock_data['Volume'].astype(int)

            #print(stock_data)
            # Seperating the wanted columns
            stock_data = stock_data[['TickerSymbol', 'Date', 'Open', 'Close', 'High', 'Low', 'Volume']]
        
            #extracting the wanted values for the entry check and the day's data
            check_ticker = stock_data.iloc[0,0]
            check_date = stock_data.iloc[0,1]
            day_open = stock_data.iloc[0,2]
            day_close = stock_data.iloc[0,3]
            day_high = stock_data.iloc[0,4]
            day_low = stock_data.iloc[0,5]
            day_volume = stock_data.iloc[0,6]
            
            # check if stock already updated for the day
            self.cursor.execute(self.stock_day_check,check_ticker, check_date)
            data_exists = self.cursor.fetchall()

            if len(data_exists) > 0:
                # if record already exists update the record
                self.cursor.execute(self.update_stock, float(day_open), float(day_close), float(day_high), float(day_low), int(day_volume), check_ticker, check_date)
                print(check_ticker, "\nStock Updated")
                self.conn.commit()
            else:
                # record is new, insert the record
                self.cursor.execute(self.insert_stock, check_ticker, check_date, float(day_open), float(day_close), float(day_high), float(day_low), int(day_volume))
                print("Day inserted")
                self.conn.commit()
    

    def dw_std_query(self, ticker):
        """ Querying 1 year worth of data from the database """
        sql_statement = """
            SELECT *
            FROM [StockDataWarehouse].dbo.StockInformation s
            WHERE s.Date BETWEEN ? AND ? AND s.TickerSymbol = ?
            ORDER BY s.Date DESC
        """
        date_start = datetime.date.today() - datetime.timedelta(days=365)
        date_end = datetime.date.today()
        self.cursor.execute(sql_statement, date_start.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d"), ticker)
        data = self.cursor.fetchall()
        return data

    def dw_check_stock(self, ticker):
        """Query to check whether stock has been updated or not"""
        self.stock_check = """ 
            SELECT *
            FROM [StockDataWarehouse].dbo.StockInformation s
            WHERE s.TickerSymbol= ?
        """
        self.cursor.execute(self.stock_check, ticker)
        data = self.cursor.fetchall()
        if len(data) == 0:
            return False
        return True
         
if __name__ == "__main__":
    test = DW_Stock()
    test.dw_update()