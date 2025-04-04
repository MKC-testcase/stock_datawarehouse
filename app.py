import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from flask import Flask, render_template, request
import yfinance as yf
from data_warehouse_operations import DW_Stock
from stock_metrics import StockMetrics
import datetime

app = Flask(__name__)
@app.route('/', methods=['GET', 'POST'])
def index():
        """Starting the web application"""
        if request.method == 'POST':
            ticker = request.form['ticker']
            # returns stock data if avaialble None else
            stock_data = fetch_stock_data(ticker)
            # starts the process to input new data into the datawarehouse
            if stock_data is None:
                datawarehouse = DW_Stock()
                result = datawarehouse.dw_setup(ticker)
                if result:
                    # collecting the stock information from the data warehouse
                    stock_data = fetch_stock_data(ticker)

            # Assuming there is no stock information available generate the graph for the web application
            if stock_data is not None:
                #
                fig = plot_stock_graph(stock_data, ticker)
                graph_html = fig.to_html(full_html=False)
                return render_template('index.html', graph_html=graph_html)
            else:
                return render_template('index.html', error="Invalid stock symbol or data not found.")

        return render_template('index.html', graph_html=None)

def fetch_stock_data(ticker):
    """ Getting the stock from the data warehouse and formatting it to include additional stock information """
    try:
        # stock = yf.Ticker(ticker)
        # data = stock.history(period="1y")
        data_warehouse_stock = DW_Stock()
        #check if the stock is in the data warehouse
        check = data_warehouse_stock.dw_check_stock(ticker)
        if not check:
            return None
        # gets data from the data warehouse within a year
        data = data_warehouse_stock.dw_std_query(ticker)
        # adding addition stock metrics
        metrics = StockMetrics()
        today = datetime.date.today()
        start_date50 = datetime.date.today() - datetime.timedelta(days=415)
        start_date200 = datetime.date.today() - datetime.timedelta(days=565)
        year_date = datetime.date.today() - datetime.timedelta(days=365)

        close, c_date, SMA50, _ = metrics.simple_moving_average(ticker, start_date50, today, 50)
        _, _, SMA200, _ = metrics.simple_moving_average(ticker, start_date200, today, 200)
        _, _, EMA50 = metrics.exponentail_moving_average(ticker,year_date, today, 50)
        _, _, EMA200 = metrics.exponentail_moving_average(ticker, year_date, today, 200)

        # converting metrics to dataframe
        # filling in the missing values that for SMA and EMA to match with the close and date
        adjusted_SMA50 = ([0] * (len(close) - len(SMA50))) + SMA50
        adjusted_SMA200 = ([0] * (len(close) - len(SMA200))) + SMA200
        adjusted_EMA50 = ([0] * (len(close) - len(EMA50))) + EMA50
        adjusted_EMA200 = ([0] * (len(close) - len(EMA200))) + EMA200

        df2 = pd.DataFrame(close, columns=['Close'])
        df2.index = c_date
        # adding the other metrics
        df2["SMA50"] = adjusted_SMA50
        df2["SMA200"] = adjusted_SMA200
        df2["EMA50"] = adjusted_EMA50
        df2["EMA200"] = adjusted_EMA200

        # need to convert data to dataframe
        df = pd.DataFrame.from_records(data=data, columns=['StockID', 'TickerSymbol', 'Date', 'OpenPrice', 
                                                           'ClosePrice', 'Highprice', 'LowPrice', 'Volume'])
        df['SMA50'] = df['ClosePrice'].rolling(window=50).mean()
        df['EMA50'] = df['ClosePrice'].ewm(span=50, adjust=False).mean()

        return df
    except Exception as e:
        print(f"Error fetching stock data: {e}")
        return None


def plot_stock_graph(stock_data, ticker):
    """ Creates a plotly figure that can be displayed on the web application as a interactive figure """
    # px.line takes dataframe, the x and y are the dataframe columns link below for reference
    fig = px.line(stock_data, x='Date', y="ClosePrice", title=f"Stock Price Over Time:  {ticker}")
    fig = go.Figure()
    # adds the closing price to the graph
    fig.add_trace(
        go.Scatter(
            x= stock_data['Date'],
            y= stock_data['ClosePrice'],
            mode="lines",
            name = 'Closing Price',
            showlegend=True
            )
        )
    # Adds the simple moving average for 50 days to the graph
    fig.add_trace(
        go.Scatter(
            x = stock_data['Date'],
            y = stock_data['SMA50'],
            mode = "lines",
            line=go.scatter.Line(color='red'),
            name='Simple Moving Average(50)'
            )
        )
    # Addes the exponential moving average for 50 days to the graph
    fig.add_trace(
        go.Scatter(
            x = stock_data['Date'],
            y = stock_data['EMA50'],
            mode = "lines",
            line=go.scatter.Line(color='green'),
            name='Exponential Moving Average(50)'
            )
        )
    fig.update_xaxes(title="Date")
    fig.update_yaxes(title="Price (USD)")

    return fig

if __name__ == "__main__":
    app.run()
