import dash
from dash.dependencies import Output, Event, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
import pymysql
import calendar;
import time
from datetime import datetime,timedelta
from pytz import timezone
import pandas as pd
import os

app = dash.Dash(__name__)

mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']
app.layout = html.Div(
    [
        html.Div(
        [
            html.Div(children='''
            Advertisement Id:
            '''),
            dcc.Input(id='input', value='140949', type='text',style={'width': '20%', 'display': 'inline-block', 'margin-left':'20px'}),
        ],style={'width': '30%', 'display': 'inline-block', 'margin-left':'50px'}),
        
        html.Div(
        [
                html.Div(children='''
                Observation Interval (in mins):
                ''',
                style={'width': '100%', 'display': 'inline-block'},
                ),
                dcc.Input(id='input2', value='2', type='text',style={'width': '10%', 'display': 'inline-block','margin-left':'20px'}),
                
        ],style={'width': '50%', 'display': 'inline-block', 'margin-left':'-220px'}),
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            # interval=8*1000,
            n_intervals=0,
        ),   
    ]
)

pstimezone = timezone('US/Pacific')
us_time = datetime.now(pstimezone)
times1 = us_time.strftime('%Y-%m-%d %H:%M:%S')
@app.callback(Output('live-graph', 'figure'),
              [Input(component_id='input', component_property='value'),Input(component_id='input2', component_property='value'),Input('graph-update', 'n_intervals')])
              #events=[Event('graph-update', 'interval')])
def update_graph_scatter(input_data,input_data2,n):
    times2 = (us_time+timedelta(minutes=int(input_data2))).strftime('%Y-%m-%d %H:%M:%S')
    
    # Connection
    conn = pymysql.connect(host="54.193.71.186",port=3306,db="Project",user=mysql_username,password=mysql_password)
    print("Connection established sucessfully")

    # Creation of a Cursor object
    cursor = conn.cursor()
    sql = "SELECT ad_id,timestamp,round((sum(clicks)/sum(views))*10,2) as ctr FROM mytopic group by ad_id,timestamp having ad_id={adid}".format(adid=input_data)
    df = pd.read_sql(sql, conn)

    cursor.close()
    conn.close()
    
    data = plotly.graph_objs.Scatter(
            x=df['timestamp'],
            y=df['ctr'],
            name='Scatter',
            mode= 'lines+markers'
            )
    
    layout = go.Layout(title=go.layout.Title(
                                            text="Test",
                                            xref='paper',
                                            x=0
                                        ),
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
                                text="CTR",
                                textangle=-90,
                                xref="paper",
                                yref="paper"
                            )
                        ],
                        title_text="Advertisement",
                        height= 500,
                        width= 700,)
    
    return {'data': [data],'layout' : layout}

if __name__ == '__main__':
    app.run_server(debug=True)