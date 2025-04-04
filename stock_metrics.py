# By: Marcus Chan
# Last Edited: 2025-03-24
# The purpose of this file is to calculate helpful stock metrics given a SQL database is already set up

import pyodbc
import datetime
import statistics # just to use standard deviation
import configparser

class StockMetrics:
    def __init__(self):
        # retrieve SQL database configuration from config file
        config = configparser.ConfigParser()
        config.read("configs\config.ini")
        # setting up the connection to the data warehouse
        self.conn = pyodbc.connect(driver='{SQL Server Native Client 11.0}',
                              server=config["Data Warehouse"]["server"],
                              database=config["Data Warehouse"]["database"],
                              uid=config["Data Warehouse"]["username"],
                              pwd=config["Data Warehouse"]["password"])
        self.cur = self.conn.cursor()
        self.c_price_plot = []
        self.dates_plot = []

    def simple_moving_average(self, ticker, start_date, end_date, window_size):
        """
        Calculates the simple moving average over a period of time set by the user
        ticker: ticker name eg "AAPL"
        start_date: Start date of the function consideration
        end_date: End date of the function consideration
        window_size: how long the simple moving average is averaging over
        """
        select_statement = """
        SELECT Date, ClosePrice
        FROM StockInformation s
        WHERE s.Date BETWEEN ? AND ? AND s.TickerSymbol = ?
        ORDER BY Date ASC
        """
        #setting the ticker to avoid confusion if we even mix functions
        self.ticker = ticker

        # querying database for the data
        self.cur.execute(select_statement, start_date, end_date, ticker)
        results = self.cur.fetchall()

        # allocating space for the data 
        closing_price = [None] * len(results) 
        dates = [None] * len(results)
        # appending the closing results to the empty arrays declared above
        for i in range(len(results)):
            dates[i] = results[i][0]
            closing_price[i] = results[i][1]

        # copying the data over to use this data later in other functions
        self.c_price_plot = closing_price.copy()
        self.dates_plot = dates.copy()

        # plot_line is essentially the SMA combined with dates can be plotted
        plot_line = [None]*(len(closing_price) - window_size)
        plot_date = [None]*(len(closing_price) - window_size)
        window_start, window_end = 0, window_size
        # 2 pointers approach
        while window_end <= len(closing_price)-1:
            # aggregating the results based on the window size
            plot_date[window_start] = dates[window_end-1]
            plot_line[window_start] = sum(closing_price[window_start: window_end])/window_size
            window_start += 1
            window_end += 1

        return self.c_price_plot, self.dates_plot, plot_line, plot_date

    def exponentail_moving_average(self, ticker, start_date, end_date, window_size):
        """
        Calculates the exponential moving average over a period of time set by the user
        ticker: ticker name eg "AAPL"
        start_date: Start date of the function consideration
        end_date: End date of the function consideration
        window_size: how long the simple moving average is averaging over
        """
        select_statement = """
        SELECT Date, ClosePrice
        FROM StockInformation s
        WHERE s.Date BETWEEN ? AND ? AND s.TickerSymbol = ?
        ORDER BY Date ASC
        """
        self.cur.execute(select_statement, start_date, end_date, ticker)
        results = self.cur.fetchall()

        closing_price = [None] * len(results) # x on our plot
        dates = [None] * len(results) # the y on our plot
        # appending the closing results to the empty arrays declared above
        for i in range(len(results)):
            dates[i] = results[i][0]
            closing_price[i] = results[i][1]

        plot_line = [None]*(len(closing_price))
        plot_line[0] = closing_price[0]
        iterator = 1

        while iterator <= len(results)-1:
            # aggregating the results based on the window size
            plot_line[iterator] = (closing_price[iterator] - plot_line[iterator-1])/(2/window_size + 1) + plot_line[iterator-1]
            iterator += 1

        return closing_price, dates, plot_line

    def reletive_strength_index(self, ticker, start_date, end_date):
        """
        Calculates the RSI index over a period of time set by the user
        ticker: ticker name eg "AAPL"
        start_date: Start date of the function consideration
        end_date: End date of the function consideration
        """
        select_statement = """
        SELECT Date, ClosePrice
        FROM StockInformation s
        WHERE s.Date BETWEEN ? AND ? AND s.TickerSymbol = ?
        ORDER BY Date ASC
        """
        self.cur.execute(select_statement, start_date, end_date, ticker)
        results = self.cur.fetchall()

        closing_price = [None] * len(results) # x on our plot
        gain = []
        loss = []
        dates = [None] * len(results) # the y on our plot
        persistent = 0
        # appending the closing results to the empty arrays declared above
        for i in range(len(results)):
            dates[i] = results[i][0]
            if results[i][1] > persistent:
                gain.append(results[i][1])
                persistent = results[i][1]
            else:
                loss.append(results[i][1])
                persistent = results[i][1]
        gain_average = sum(gain)/len(gain)
        loss_average = sum(loss)/len(loss)

        return round(100 - (100/(1+(gain_average/loss_average))), 4)

    def bollinger_bands(self, ticker, start_date, end_date):
        """
        Calculates the bollinger bands over a period of time set by the user
        ticker: ticker name eg "AAPL"
        start_date: Start date of the function consideration
        end_date: End date of the function consideration
        """
        closing_p, c_date, SMA, s_dates = self.simple_moving_average(ticker, start_date, end_date, 20)
        start, end = 0, 20
        deviate_rates = [None] * (len(closing_p) - 20)
        while end < len(SMA):
            deviate_rates[start] = statistics.stdev(closing_p[start:end])
            start += 1
            end += 1
        upper = [None] * (len(closing_p) - 20)
        lower = [None] * (len(closing_p) - 20)
        for i in range(len(SMA)):
            upper[i] = SMA[i] + 2 * deviate_rates[i]
            lower[i] = SMA[i] - 2 * deviate_rates[i]

        return upper, lower, SMA, s_dates
     
if __name__ == "__main__":
    test = StockMetrics()
    nums1, dat1, num2, dat2 = test.simple_moving_average("AAPL", "2022-01-01", "2022-01-31", 7)
   
    
