import dash
from dash import html, dcc
import plotly.express as px
import pandas as pd

app = dash.Dash(__name__)


def fetch_data():
    return pd.read_excel('stockdata_stocks.xlsx', engine='openpyxl')


df = fetch_data()

bar_fig = px.bar(df, x='trade_date', y='price', color='trade_type',
                 barmode='group', title='Price by Trade Date and Type')
line_fig = px.line(df, x='trade_date', y='price', title='Price Over Time')

scatter_fig = px.scatter(df, x='quantity', y='price',
                         color='trade_type', title='Price vs Quantity')

area_fig = px.area(df, x='trade_date', y='price',
                   title='Price Trends Over Time')
pie_fig = px.pie(df, names='trade_type', values='price',
                 title='Price Distribution by Trade Type')
histogram_fig = px.histogram(
    df, x='price', nbins=20, title='Price Distribution')
box_fig = px.box(df, y='price', x='trade_type',
                 title='Price Distribution by Trade Type')

app.layout = html.Div(children=[
    html.H1(children=' RealTime StockStream 💵🕊️'),

    dcc.Graph(id='bar-chart', figure=bar_fig),
    dcc.Graph(id='line-chart', figure=line_fig),
    dcc.Graph(id='scatter-plot', figure=scatter_fig),
    dcc.Graph(id='area-chart', figure=area_fig),
    dcc.Graph(id='pie-chart', figure=pie_fig),
    dcc.Graph(id='histogram', figure=histogram_fig),
    dcc.Graph(id='box-plot', figure=box_fig)
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
