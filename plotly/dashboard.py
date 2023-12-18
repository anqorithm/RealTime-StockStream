import dash
from dash import html, dcc
import plotly.express as px
import pandas as pd
from cassandra.cluster import Cluster

app = dash.Dash(__name__)


def create_cassandra_session():
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect('stockdata')
    return session


def fetch_data_from_cassandra(table_name):
    session = create_cassandra_session()
    query = f"SELECT * FROM {table_name}"
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))
    return df


def create_figure(df, chart_type, **kwargs):
    if df.empty:
        print(f"No data available for {kwargs.get('title')}")
        return dcc.Graph()
    else:
        if chart_type == 'bar':
            return px.bar(df, **kwargs)
        elif chart_type == 'line':
            return px.line(df, **kwargs)
        elif chart_type == 'scatter':
            return px.scatter(df, **kwargs)
        elif chart_type == 'area':
            return px.area(df, **kwargs)
        elif chart_type == 'pie':
            return px.pie(df, **kwargs)


stocks_df = fetch_data_from_cassandra('stocks')
grouped_stocks_df = fetch_data_from_cassandra('grouped_stocks')
pivoted_stocks_df = fetch_data_from_cassandra('pivoted_stocks')
ranked_stocks_df = fetch_data_from_cassandra('ranked_stocks')
analytics_stocks_df = fetch_data_from_cassandra('analytics_stocks')

bar_fig = create_figure(stocks_df, 'bar', x='trade_date', y='price',
                        color='trade_type', title='Price by Trade Date and Type')
line_fig = create_figure(grouped_stocks_df, 'line', x='trade_type',
                         y='avg_price', title='Average Price by Trade Type')
scatter_fig = create_figure(ranked_stocks_df, 'scatter', x='quantity',
                            y='price', color='trade_type', title='Price vs Quantity')
area_fig = create_figure(pivoted_stocks_df, 'area', x='stock', y=[
                         'avg_price_buy', 'avg_price_sell'], title='Average Buy vs Sell Price')
pie_fig = create_figure(analytics_stocks_df, 'pie', names='trade_type',
                        values='avg_price_overall', title='Overall Average Price Distribution by Trade Type')


histogram_fig = create_figure(
    stocks_df, 'bar', x='quantity', title='Stock Quantity Distribution')

box_plot_fig = create_figure(
    stocks_df, 'box', x='trade_type', y='price', title='Price Analysis by Trade Type')

heatmap_fig = create_figure(stocks_df, 'heatmap', x='price',
                            y='quantity', title='Price and Quantity Correlation')

time_series_fig = create_figure(pivoted_stocks_df, 'line', x='stock', y=[
                                'avg_price_buy', 'avg_price_sell'], title='Time Series of Buy vs Sell Prices')

candlestick_fig = create_figure(stocks_df, 'candlestick', x='trade_date', open='open_price',
                                high='high_price', low='low_price', close='close_price', title='Stock Price Movements')

treemap_fig = create_figure(analytics_stocks_df, 'treemap', path=[
                            'trade_type', 'stock'], values='quantity', title='Treemap of Stock Distribution')

sunburst_fig = create_figure(grouped_stocks_df, 'sunburst', path=[
                             'trade_type', 'stock'], values='avg_price', title='Sunburst Chart of Stock Categories')

violin_fig = create_figure(ranked_stocks_df, 'violin', x='trade_type',
                           y='price', title='Violin Plot of Price Distribution')

app.layout = html.Div(children=[
    html.H1(children='RealTime StockStream 💵🕊️'),
    dcc.Graph(id='line-chart', figure=line_fig),
    dcc.Graph(id='scatter-plot', figure=scatter_fig),
    dcc.Graph(id='area-chart', figure=area_fig),
    dcc.Graph(id='pie-chart', figure=pie_fig),
    dcc.Graph(id='histogram-chart', figure=histogram_fig),
    dcc.Graph(id='box-plot-chart', figure=box_plot_fig),
    dcc.Graph(id='heatmap-chart', figure=heatmap_fig),
    dcc.Graph(id='time-series-chart', figure=time_series_fig),
    dcc.Graph(id='candlestick-chart', figure=candlestick_fig),
    dcc.Graph(id='treemap-chart', figure=treemap_fig),
    dcc.Graph(id='sunburst-chart', figure=sunburst_fig),
    dcc.Graph(id='violin-chart', figure=violin_fig)
    # Add more graphs here as needed
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=6060)
