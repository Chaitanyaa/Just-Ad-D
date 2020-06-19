import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
import pymysql
import calendar;
import time
import dash_table
from datetime import datetime,timedelta
from pytz import timezone
import pandas as pd
import os
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']

# Connection
conn = pymysql.connect(host="54.193.71.186",port=3306,db="Project",user=mysql_username,password=mysql_password)
print("Connection established sucessfully")

# Creation of a Cursor object
cursor = conn.cursor()
sql = "select document_id as website_id,users as user_traffic from page_views"
df = pd.read_sql(sql, conn)

sql = "select *from page_views_platform"
df1 = pd.read_sql(sql, conn)

sql = "select *from page_views_traffic"
df2 = pd.read_sql(sql, conn)

cursor.close()
conn.close()

app.layout = html.Div(
    [
        html.Div(
        [
            html.Div(children='''
            Website Id
            '''),
            dcc.Input(id='webid', value='66721705', type='text',style={'width': '20%', 'display': 'inline-block', 'margin-left':'-2px'}),
        ],style={'width': '30%', 'display': 'inline-block', 'margin-left':'50px'}),
        html.Div(
        [
            html.Div(children='''
            Advertisement Id
            '''),
            dcc.Input(id='input', value='140949', type='text',style={'width': '20%', 'display': 'inline-block', 'margin-left':'20px'}),
        ],style={'width': '30%', 'display': 'inline-block', 'margin-left':'-200px'}),
        
        html.Div(
        [
            html.Div(children='''
            Observation Interval (in mins)
            '''),
            dcc.Input(id='input2', value='2', type='text',style={'width': '20%', 'display': 'inline-block', 'margin-left':'20px'}),
        ],style={'width': '30%', 'display': 'inline-block', 'margin-left':'-200px'}),
        
        html.Div(
        [
        dcc.Graph(id='live-graph', animate=True,style={'width': '50%', 'display': 'inline-block'}),
        dcc.Interval(
            id='graph-update',
            n_intervals=0,
        ),
        html.H1(children='''Top websites with user traffic'''),
        html.P([html.Button('Refresh', id='refresh',style={'display': 'inline-block'}),html.Button('Extend', id='extend',style={'display': 'inline-block','margin-left':'50px'})]),
        dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict('records')
        ),
        ],style={'width': '80%', 'display': 'inline-block', 'margin-left':'10px'}),

        dcc.Graph(
        id='platform',
        figure={
            'data': [
                {'x': df1['platform'], 'y': df1['users'], 'type': 'bar', 'name': 'Platform Distribution'},
            ],
            'layout': {
                'title': 'Platform Distribution',
            }
        },style={'width': '80%', 'display': 'inline-block', 'margin-left':'10px'}),
        
        dcc.Graph(
        id='traffic',
        figure={
            'data': [
                {'x': df2['traffic_source'], 'y': df2['users'], 'type': 'bar', 'name': 'Platform Distribution'},
            ],
            'layout': {
                'title': 'Traffic Distribution',
            }
        },style={'width': '80%', 'display': 'inline-block', 'margin-left':'10px'}),
    ],style={'margin-left':'220px'}
)

pstimezone = timezone('US/Pacific')
us_time = datetime.now(pstimezone)
times1 = us_time.strftime('%Y-%m-%d %H:%M:%S')
@app.callback(Output('live-graph', 'figure'),
              [Input(component_id='input', component_property='value'),Input(component_id='input2', component_property='value'),Input('graph-update', 'n_intervals')])
def update_graph_scatter(input_data,input_data2,n):
    times2 = (us_time+timedelta(minutes=int(input_data2))).strftime('%Y-%m-%d %H:%M:%S')
    
    # Connection
    conn = pymysql.connect(host="54.193.71.186",port=3306,db="Project",user=mysql_username,password=mysql_password)
    print("Connection established sucessfully")

    # Creation of a Cursor object
    cursor = conn.cursor()
    sql = '''
    SELECT ad_id,
           timestamp,
           round((sum(clicks)/sum(views))*10,2) as ctr 
    FROM mytopic 
    group by ad_id,
            timestamp 
    having ad_id={adid}'''.format(adid=input_data)

    df = pd.read_sql(sql, conn)

    cursor.close()
    conn.close()
    
    data = plotly.graph_objs.Scatter(
            x=df['timestamp'],
            y=df['ctr'],
            name='Scatter',
            mode= 'lines+markers'
            )
    
    layout = go.Layout(
                        xaxis=dict(range=[times1,times2]),
                        yaxis=dict(range=[0,15]),
                        annotations=[
                            dict(
                                x=0.5,
                                y=-0.15,
                                showarrow=False,
                                text="Timestamp",
                                xref="paper",
                                yref="paper"
                            ),
                            dict(
                                x=-0.07,
                                y=0.5,
                                showarrow=False,
                                text="Click Through Rate(%)",
                                textangle=-90,
                                xref="paper",
                                yref="paper"
                            )
                        ],
                        height= 400,
                        width= 900,)
    
    return {'data': [data],'layout' : layout}


@app.callback(Output('table', 'data'),
              [Input('refresh', 'n_clicks')])
def update_data(n_clicks):

    # Connection
    conn = pymysql.connect(host="54.193.71.186",port=3306,db="Project",user=mysql_username,password=mysql_password)
    print("Connection established sucessfully")

    # Creation of a Cursor object
    cursor = conn.cursor()
    sql = "select document_id as website_id,users as user_traffic from page_views"
    df = pd.read_sql(sql, conn)

    cursor.close()
    conn.close()

    return df.to_dict('records')
    
if __name__ == '__main__':
    app.run_server(debug=True)