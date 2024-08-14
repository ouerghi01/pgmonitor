import pandas as pd
import matplotlib.pyplot as plt
from dash  import Dash , dcc , html,dash_table,callback,Output,Input 
from aiokafka import AIOKafkaConsumer

import asyncio
from sklearn.ensemble import IsolationForest
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import dash_bootstrap_components as dbc
import threading
import plotly.express as px
from kafka import KafkaConsumer
import logging
from joblib import load, dump
import time
from queue import Queue 
import threading
import os 
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from ProducerConsumer.Notify import EmailSender
from ProducerConsumer.Anomaly_detection_pipeline.anomaly_detection import AnomalyDetectionPipeline  
import json 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConsumerVisualizer:
    def __init__(self,_event_stop):
       
        self.thread_pool = ThreadPoolExecutor(max_workers=10)
        self.consumer =KafkaConsumer("db-monitoring", bootstrap_servers='localhost:9092',auto_offset_reset='earliest',enable_auto_commit=True,value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.ai_consumer = None
        self.activities = pd.DataFrame(columns=['datetimeutc', 'pid', 'database', 'appname', 'user', 'client', 'cpu', 'memory', 'read', 'write', 'duration', 'wait', 'io_wait', 'state', 'query'])
        self.pg_stat_activity = pd.DataFrame(columns=['datid', 'datname', 'pid', 'usesysid', 'usename', 'application_name', 'client_addr', 'client_hostname', 'client_port', 'backend_start', 'xact_start', 'query_start', 'state_change', 'wait_event_type', 'wait_event', 'state', 'backend_xid', 'backend_xmin', 'query', 'info'])
        self.pg_stat_user_tables = pd.DataFrame(columns=['schemaname','table_name','total_size','table_size','indexes_size','sequential_scans'])
        self.app = Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions=True)
        sidebar = html.Div(
        [
        dbc.Nav(
            [
            dbc.NavLink("Overview", href="/Overview", active="exact",id="overview-link"),
            dbc.NavLink("Query Performance", href="/Query_Performance", active="exact"),
            dbc.NavLink("Activity Monitoring", href="/Activity_Monitoring", active="exact"),
            dbc.NavLink("I/O Statistics", href="/IO_Statistics", active="exact"),
            dbc.NavLink("Transaction Stats", href="/Transaction_Stats", active="exact"),
            dbc.NavLink("Buffer and Cache", href="/Buffer_and_Cache", active="exact"),
            dbc.NavLink("WAL & Replication", href="/WAL_Replication", active="exact"),
            dbc.NavLink("Connections", href="/Connections", active="exact"),
            dbc.NavLink("Error Logs", href="/Error_Logs", active="exact"),
            dbc.NavLink("Custom Alerts", href="/Custom_Alerts", active="exact"),
            ],
            vertical=True,
            pills=True,
        )
        ],
        style={"position": "fixed", "left": 0, "top": 0, "bottom": 0, "width": "250px", "padding": "2rem 1rem", "background-color": "#333"}
         )
        self.app.layout =dbc.Container([
            dbc.Row([
                dbc.Col(sidebar, width=2),
                dbc.Col([
                    dcc.Location(id='url', refresh=False),
                    html.Div(id="page-content"),
                ], width=10)
            ]),dcc.Interval(id='interval', interval=1000, n_intervals=0)
        ]
        )
        self.data_lock = threading.Lock()
        self.out_q = Queue()
        self.event_stop = _event_stop
        self.retrain_interval = 100  # Retrain after every 100 messages
        self.message_count = 0
        self.emailSender=EmailSender()
        self.initialized=False
        #self.pipeline=AnomalyDetectionPipeline(columns=self.activities.columns,path='ProducerConsumer/Anomaly_detection_pipeline/models/xprocessor.pkl')
        #self.new_pieline=AnomalyDetectionPipeline(columns=self.activities.columns,path='ProducerConsumer/Anomaly_detection_pipeline/models/xprocessor_1.pkl')
    async def setup(self):
        if not self.initialized:

          try:
             self.ai_consumer = AIOKafkaConsumer('query-monitoring', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',enable_auto_commit=True,value_deserializer=lambda x: json.loads(x.decode('utf-8')))
             await self.ai_consumer.start()
          except Exception  as e:
              logger.exception("Error async consuming %s", e)
        self.initialized=True
              
       

    def consume_messages(self):
      while not self.event_stop.is_set():
        messages = self.consumer.poll(timeout_ms=1000)
        if messages:
            with self.data_lock:
              for _, msgs in messages.items():
                  for message in msgs:
                     topic=message.topic
                     func_dict={"db-monitoring":self.handle_anomaly}
                     func_dict[topic](message)

      time.sleep(0.01)
    async def ai_consume_message(self):
        logger.info("ai_consumer started\n")
        await self.setup()
        try:

           async for message in self.ai_consumer:
            topic=message.topic
            func_dict={"query-monitoring":self.handle_query}
            func_dict[topic](message)
        except Exception as e:
            logger.exception("Error in AI consumer: %s", e)
        finally:
            await self.ai_consumer.stop()
    
    def handle_query(self, message):

        data=message.value
        type_query=data['type_query']
        
        record=data['data']
        func_dict={ "pg_stat_user_tables":self.process_data_pg_stat_user_tables,"pg_stat_activity":self.process_data_stat_activity}
        func_dict[type_query](record)
            #print("shape of pg_stat_user_tables",self.pg_stat_user_tables.shape)
    def process_data_stat_activity(self,record):
        for item in record:
            new_row = pd.DataFrame([item])
            with self.data_lock:
                self.pg_stat_activity = pd.concat([self.pg_stat_activity, new_row], ignore_index=True)
    def process_data_pg_stat_user_tables(self,record):
        for item in record:
            new_row = pd.DataFrame([item])
            with self.data_lock:
                self.pg_stat_user_tables = pd.concat([self.pg_stat_user_tables, new_row], ignore_index=True)
    def handle_anomaly(self, message):
        data = message.value 
        row=pd.DataFrame([data])
        row_activity = row.copy() # use this for prediction
        #prediction = self.pipeline.final_predection(self.transform_row_data(row))
        self.message_count+=1
                     
        #print('Anomaly type:', prediction["predicted_label"])
        #self.emailSender.sendAnyData(json.dumps(
                            #{
                            #   'alert': prediction["predicted_label"],
                            #}
                         #))
        #row_activity['predicted_label'] = prediction["predicted_label"]
        #row_activity['anomaly_scores'] = prediction["anomaly_scores"]
        self.activities = pd.concat([self.activities, row_activity], ignore_index=True)
                     
        '''
                     if self.activities.shape[0] > 10:
                          if "predicted_label" and "anomaly_scores" in self.activities.columns:
                             self.activities.drop(columns=['predicted_label','anomaly_scores'], inplace=True)
                          prediction_data = self.pipeline.final_predection(self.transform_row_data(self.activities.dropna()))
                          self.activities['predicted_label'] = prediction_data['predicted_label']
                          self.activities['anomaly_scores'] = prediction_data['anomaly_scores']
                     if self.message_count % self.retrain_interval == 0:
                        pass
                          #if "anomaly" in self.activities.columns:
                             #self.activities.drop(columns=['anomaly'], inplace=True)
                          #thread_retain=threading.Thread(target=self.retrain_models)
                          #thread_retain.start()
                     '''
                     
        #self.convert_activities()
    def retrain_models(self):
        print("training start\n")
        with self.data_lock:
          Dataset=self.activities.copy()
          isolation_forest = IsolationForest(n_estimators=100, contamination='auto')
          Data=self.prepare_data_for_prediction(Dataset)
          isolation_forest.fit(Data.values)
          dump(isolation_forest, os.path.abspath('/home/aziz/DBWatch/Stage_Bri/ProducerConsumer/models/isolation_forest.joblib'))
          self.isolation_forest = isolation_forest
    def transform_row_data(self, row):
        row['cpu'] = pd.to_numeric(row['cpu'], errors='coerce')
        row['memory'] = pd.to_numeric(row['memory'], errors='coerce')
        row['read'] = pd.to_numeric(row['read'], errors='coerce')
        row['write'] = pd.to_numeric(row['write'], errors='coerce')
        row['duration'] = pd.to_numeric(row['duration'], errors='coerce') 
        row['datetimeutc'] = pd.to_datetime(row['datetimeutc'], errors='coerce') 
        row['pid']=pd.to_numeric(row['pid'], errors='coerce')
        return row

    def convert_activities(self):
        self.activities['cpu']=pd.to_numeric(self.activities['cpu'],downcast='float')
        #self.activities['query'] = self.activities['query'].apply(lambda x : x.replace('$',''))
        #self.activities['query'] = self.activities['query'].apply(lambda x: str(x[:8]) +'...')
        self.activities['duration']=pd.to_numeric(self.activities['duration'],downcast='float')
        self.activities['memory']=pd.to_numeric(self.activities['memory'],downcast='float')
        self.activities['read']=pd.to_numeric(self.activities['read'],downcast='float')
        self.activities['write']=pd.to_numeric(self.activities['write'],downcast='float')
    def run_server(self):
        @self.app.callback(
            Output("page-content", "children"),
            [Input("url", "pathname"),Input('interval', 'n_intervals') ],
        )
        def render_page_content(n,n1):
            if n == "/Overview":
                return dbc.Container([
                    dbc.Row([
                        html.H1("Postgres Table Overview",style={
                            "textAlign": "center",           # Center the text
                            "color": "red",                 # Dark gray color for the text
                            "fontSize": "2.5rem",            # Responsive font size
                            "margin": "20px 0",              # Margin above and below the heading
                            "fontFamily": "Arial, sans-serif", # Font family for a clean look
                            "fontWeight": "bold",            # Bold text
                            "backgroundColor": "#f8f9fa",    # Light gray background
                            "padding": "10px",               # Padding around the text
                            "borderRadius": "5px",           # Rounded corners
                            "boxShadow": "0 2px 4px rgba(0, 0, 0, 0.1)"  # Subtle shadow for depth
                        }),
                        dbc.Col(
                            [
                                html.H4(id="category_title", style={"text-align": "center", "color": "blue"}),
                                dcc.Graph(
                                    id="pie_chart",
                                    figure={}
                                ),
                                dcc.Dropdown(
                                    id="values",
                                    options=[{'label': 'Total Size', 'value': 'total_size'},{'label': 'Table Size', 'value': 'table_size'},{'label': 'Indexes Size', 'value': 'indexes_size'}],
                                    value='total_size',
                                    clearable=False,
                                    
                                ),
                            ],
                            width=4,
                            
                        ),
                        dbc.Col(
                            [
                                html.H4("Postgres Activities"),
                                dcc.Graph(id='system_load_line', figure={}),
                                dcc.Dropdown(
                                    id='load_parameters_y',
                                    options=[
                                        {'label': 'Memory', 'value': 'memory'},
                                        {'label': 'Write', 'value': 'write'},
                                        {'label': 'Read', 'value': 'read'}
                                    ],
                                    value='memory',
                                    clearable=False
                                ),
                            ],
                            width=8,
                            style={
                                "padding": "1rem",
                                "background-color": "#f8f9fa",
                                "border-radius": "8px",
                                "box-shadow": "0px 4px 6px rgba(0,0,0,0.1)",  # Subtle shadow for depth
                                "margin-bottom": "1rem",  # Space below the column
                            }
                        )
                    ])
                ])
            elif n == "/Query_Performance":
                query_execution_time_distribution=self.activities.groupby(['query','datetimeutc'],as_index=False).agg({'duration':'mean'}).reset_index()
                query_execution_time_distribution['datetimeutc'] = pd.to_datetime(query_execution_time_distribution['datetimeutc'])
                return dbc.Container([
                    html.H1(
                        "Postgres Performance Analysis",
                        style={
                            "textAlign": "center",           # Center the text
                            "color": "#333",                 # Dark gray color for the text
                            "fontSize": "2.5rem",            # Responsive font size
                            "margin": "20px 0",              # Margin above and below the heading
                            "fontFamily": "Arial, sans-serif", # Font family for a clean look
                            "fontWeight": "bold",            # Bold text
                            "backgroundColor": "#f8f9fa",    # Light gray background
                            "padding": "10px",               # Padding around the text
                            "borderRadius": "5px",           # Rounded corners
                            "boxShadow": "0 2px 4px rgba(0, 0, 0, 0.1)"  # Subtle shadow for depth
                        }
                    ),
                    dbc.Row([
                        dbc.Col([
                            dcc.Graph(
                                id='top_10_queries',
                                figure={}
                            )
                        ], width={"size": 12, "md": 6})  # Full width on small screens, half width on medium and larger screens
                    ]),
                    dbc.Row([
                        dbc.Col([
                            dcc.Graph(
                                id='query_execution_time_distribution',
                                figure={}
                            )
                        ], width={"size": 12, "md": 6}),  # Full width on small screens, half width on medium and larger screens
                
                        dbc.Col([
                            dcc.RangeSlider(
                                id='time-slider',
                                min=pd.to_datetime(query_execution_time_distribution['datetimeutc']).min().timestamp(),
                                max=pd.to_datetime(query_execution_time_distribution['datetimeutc']).max().timestamp(),
                                value=[query_execution_time_distribution['datetimeutc'].min().timestamp(), query_execution_time_distribution['datetimeutc'].max().timestamp()],
                                marks={str(date): str(date) for date in pd.date_range(query_execution_time_distribution['datetimeutc'].min(), query_execution_time_distribution['datetimeutc'].max(), freq='D')},
                                tooltip={"placement": "bottom", "always_visible": True}  # Display tooltips for better user interaction
                            )
                        ], width={"size": 12, "md": 6})  # Full width on small screens, half width on medium and larger screens
                    ])
                ], fluid=True)  # Set container to fluid to ensure it scales with the viewport width

            elif n == "/Activity_Monitoring":
                return dbc.Container([
                    html.H1(
                    'Time Series Analysis Dashboard',
                        style={
                            "textAlign": "center",           # Center the text
                            "color": "#333",                 # Dark gray color for the text
                            "fontSize": "2.5rem",            # Responsive font size
                            "margin": "20px 0",              # Margin above and below the heading
                            "fontFamily": "Arial, sans-serif", # Font family for a clean look
                            "fontWeight": "bold",            # Bold text
                            "backgroundColor": "#f8f9fa",    # Light gray background
                            "padding": "10px",               # Padding around the text
                            "borderRadius": "5px",           # Rounded corners
                            "boxShadow": "0 2px 4px rgba(0, 0, 0, 0.1)"  # Subtle shadow for depth
                        }
                    ),
                    dbc.Row([
                        dbc.Col([
                        dash_table.DataTable(id='activities_table', page_size=3, page_action='native',
                        style_cell={
                            'textAlign': 'left',
                            'border': '1px solid rgba(0,0,0,0.4)',
                            'boxShadow': '10px 10px 5px 0px gray',
                            'fontFamily': 'Arial, sans-serif',
                            'fontSize': '14px',
                            'padding': '10px'
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight': 'bold'
                        },
                        style_data={
                            'whiteSpace': 'normal',
                            'height': 'auto'
                        },
                        style_table={
                            'overflowX': 'auto'
                        }),
                        dcc.Interval(id='interval', interval=1000, n_intervals=0)
                        ], width={"size": 12, "md": 6})
                        ,
                        
                    ]),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dcc.Graph(
                                        id='area_chart',
                                        figure={}
                                    )
                                ],
                                width=4
                            ),
                            dbc.Col(
                                [
                                    dcc.Graph(
                                        id='line_chart',
                                        figure={}
                                    )
                                ],
                                width=4
                            ),
                            dbc.Col(
                                [
                                    dcc.Graph(
                                        id='queries_executed_by_different_users',
                                        figure={}
                                    )
                                ],
                                width=4
                            )

                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dcc.Graph(
                                        id='stacked_bar_chart',
                                        figure={}
                                    )
                                ],
                                width=12
                            )
                        ]
                    )
                    
                ], fluid=True)
            elif n == "/IO_Statistics":
                return html.P("This is the content of the I/O statistics page!")
            elif n == "/Transaction_Stats":
                return html.P("This is the content of the transaction stats page!")
            elif n == "/Buffer_and_Cache":
                return html.P("This is the content of the buffer and cache page!")
            elif n == "/WAL_Replication":
                return html.P("This is the content of the WAL & replication page!")
            elif n == "/Connections":
                return html.P("This is the content of the connections page!")
            elif n == "/Error_Logs":
                return html.P("This is the content of the error logs page!")
            elif n == "/Custom_Alerts":
                return html.P("This is the content of the custom alerts page!")
            else:
                return html.P("This is the content of the overview page!")
        @self.app.callback(
            Output("top_10_queries", "figure"),
            Output("query_execution_time_distribution", "figure"),
            [Input("url", "pathname"),Input("time-slider", "value")],
        )
        def update_query_performance(n, selected_range):
            if n == "/Query_Performance":
                # Convert `datetimeutc` to timezone-naive datetime
                self.activities['datetimeutc'] = pd.to_datetime(self.activities['datetimeutc']).dt.tz_localize(None)
                start_date = pd.to_datetime(selected_range[0], unit='s').tz_localize(None)
                end_date = pd.to_datetime(selected_range[1], unit='s').tz_localize(None)
                self.activities['query']=self.activities['query'].apply(lambda x:str(x[:10]))
                top_10_queries = self.activities.groupby('query').agg({'cpu': 'mean','write':'mean','read':'mean','memory':'sum'}).sort_values('cpu', ascending=False).head(10)
                query_execution_time_distribution=self.activities.groupby(['query','datetimeutc'],as_index=False).agg({'duration':'mean'}).reset_index()
                filtered_data = query_execution_time_distribution[
                    (query_execution_time_distribution['datetimeutc'] >= start_date) &
                    (query_execution_time_distribution['datetimeutc'] <= end_date)
                ]
                bin_size = np.ceil(np.sqrt(len(query_execution_time_distribution)))
                fig_query_execution_time_distribution = px.histogram(
                filtered_data,
                x='duration',
                color='query',
                marginal='rug',
                title='Query Execution Time Distribution'
                )
                
                fig_top_10_queries = px.bar(
                    top_10_queries,
                    x='cpu',
                    y=top_10_queries.index,
                    text='cpu',
                    labels={'cpu': 'Average CPU Time', 'query': 'Query'},
                    hover_data={'memory': True, 'read': True, 'write': True},
                    orientation='h',
                    title='Top 10 Queries by CPU Time'
                )
                
                color_map = {query: f'hsl({i * 25 % 360}, 70%, 70%)' for i, query in enumerate(top_10_queries.index)}
                fig_top_10_queries.update_traces(marker_color=[color_map[query] for query in top_10_queries.index])
                fig_top_10_queries.update_layout(
                                xaxis_title='Average CPU Time',
                                yaxis_title='Query',
                                hovermode='closest'
                                )
                return fig_top_10_queries, fig_query_execution_time_distribution
            return {}
        @self.app.callback(
            Output("activities_table", "data"),
            Output("area_chart", "figure"),
            Output("line_chart","figure"),
            Output("queries_executed_by_different_users","figure"),
            Output("stacked_bar_chart","figure"),
            [Input("url", "pathname")],
        )
        def update_activities_table(n):
            if n == "/Activity_Monitoring":
                pg_stat_activity=self.pg_stat_activity.copy()
                pg_stat_activity['query_start']=pd.to_datetime(pg_stat_activity['query_start'])
                df_choropleth=pg_stat_activity.groupby(['client_addr','query']).size().reset_index(name='count')
                choropleth_fig=px.choropleth(df_choropleth,locations='client_addr',locationmode='ISO-3',color='count',hover_name='client_addr',title='Client Address Distribution')
                df_line = pg_stat_activity.groupby([pd.Grouper(key='query_start', freq='min')]) \
            .agg({'query': lambda x: '<br>'.join(x), 'query_start': 'count'}) \
            .rename(columns={'query_start': 'query_count'}) \
            .reset_index()
                df_bar=pg_stat_activity.groupby(['backend_type','usename']).size().reset_index(name='query_count')

                line_fig = px.line(df_line, x='query_start', y='query_count', title='Query Activity Over Time')
                area_fig = px.area(df_line, x='query_start', y='query_count', title='Volume of Queries Over Time',line_group='query',color="query",labels={'query_start':'Time','query_count':'Query Count'})
                stacked_bar_fig = px.bar(df_bar, x='usename', y='query_count', color='backend_type', title='Queries by User and Backend Type', labels={'usename': 'User', 'query_count': 'Query Count'})
                currently_active_queries=self.activities[self.activities['state']=='active'][['query','state','duration']]
                area_fig.update_traces(hovertemplate='<b>Time:</b> %{x}<br><b>Query Count:</b> %{y}<br><b>Queries:</b><br>%{customdata[0]}<extra></extra>')

                area_fig.update_layout(
                    autosize=True,
                    height=500,
                    width=700,
                    margin=dict(l=40, r=40, t=40, b=40),
                    hovermode='x',
                    legend=dict(
                        orientation="v",  # Vertical legend to save horizontal space
                        yanchor="middle",
                        y=0.5,
                        xanchor="left",
                        x=1.05,  # Move legend to the right of the chart
                        font=dict(size=10)  # Reduce legend font size
                    ),
                    xaxis=dict(
                        title='Time',
                        automargin=True
                    ),
                    yaxis=dict(
                        title='Query Count',
                        automargin=True
                    ),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                )
                currently_active_queries['duration'] = currently_active_queries['duration'].astype(str) + ' second'
                currently_active_queries['state']='Running'
                return currently_active_queries.to_dict('records'),area_fig,line_fig,stacked_bar_fig,choropleth_fig
            
            
        @self.app.callback(
            Output("pie_chart", "figure"),
            Output("category_title", "children"),
            Output("system_load_line", "figure"),
            [Input("url","pathname"), Input("values", "value"), Input("load_parameters_y", "value")],
        )
        def update_overview_chart(n,value,value_y):
            try:
                if n != "/Overview":
                    return {}, "",{}
                elif value and value_y is None:
                    return {}, "Table Overview",{}
                
                try:

                    def remove_MB_KB(value):
                        return str(value.replace(' MB', '').replace(' kB', '').replace(' bytes','').strip())
                    if(self.activities.shape[0] > 0):
                       for column in ['cpu', 'memory', 'read', 'write']:
                           self.activities[column] = pd.to_numeric(self.activities[column], errors='coerce', downcast='float')
                    else:
                        raise ValueError
                    data_by_table = self.pg_stat_user_tables[['table_name','schemaname', value]].copy()
                    
                    data_by_table[value] = data_by_table[value].apply(lambda x: remove_MB_KB(x))
                    data_by_table[value] = pd.to_numeric(data_by_table[value], downcast='float')
                    
                    self.activities['query']=self.activities['query'].apply(lambda x:x[:5])
                    self.activities['duration']=pd.to_numeric(self.activities['duration'],downcast='float')
                    self.activities['info']='Duration: '+self.activities['duration'].astype(str)+'s, Wait: '+self.activities['wait'].astype(str)+'s'
                    data_operations = self.activities.groupby(['query','info'],as_index=False).agg({'cpu': 'mean', 'memory': 'mean', 'write': 'mean', 'read': 'mean'}).reset_index().head(10)
                except Exception as e:
                    print(e)
                    raise e
                threshold_size = 200
                exceeding_tables = data_by_table[data_by_table[value] > threshold_size]['table_name'].tolist()
                
                pie_chart_figure = px.pie(
                    data_by_table,
                    values=value,
                    names='table_name',
                    title="Table Overview by Size",
                    color_discrete_map={
                        table: 'red' if table in exceeding_tables else 'blue'
                        for table in data_by_table['table_name']
                    }
                )
                
                for index, table in enumerate(exceeding_tables):
                    row = data_by_table[data_by_table['table_name'] == table].iloc[0]
                    size = row[value]
                    schemaname = row['schemaname']
                    percentage = size / data_by_table[value].sum() * 100
                    annotation_x = 0.5+0.5*np.cos(index/len(exceeding_tables)*2*np.pi)
                    annotation_y =0.5+0.5*np.sin(index/len(exceeding_tables)*2*np.pi)
                    pie_chart_figure.add_annotation(
                        text='⚠️',
                        x=annotation_x,
                        y=annotation_y,
                        xref='paper',
                        yref='paper',
                        showarrow=True,
                        font=dict(size=14, color='black'),
                        arrowhead=1,
                        ax=0,
                        ay=-40,
                        hovertext=f"{row['table_name']} exceeds {threshold_size}MB ({percentage:.1f}%)",  # Tooltip text on hover
                        hoverlabel=dict(bgcolor="white", font_size=14, font_family="Arial")
                    )
                
                fig_system_load = px.line(
                    data_operations,
                    x='cpu',
                    y=value_y,
                    color='query',
                    text='info',  # Hover to show details
                    title='Operations Load',
                    markers=True,
                    width=800,  # Increased width
                    height=500   # Increased height
                )
                
                pie_chart_figure.update_layout(autosize=True,font=dict(size=12),margin=dict(t=50, l=25, r=25, b=25))
                fig_system_load.update_traces(textposition='top center')
                fig_system_load.update_layout(hovermode='closest')

                return pie_chart_figure, f"Table  by {value}",fig_system_load
            
            except Exception as e:
                print(e)
                raise e
        self.app.run_server(debug=False)
         
    def Consumer_Data_Monitoring(self):
        asyncio.run(self.async_tasks())

    async def async_tasks(self):
        async_task = asyncio.create_task(self.ai_consume_message())
        server_future = self.thread_pool.submit(self.run_server)
        consume_future = self.thread_pool.submit(self.consume_messages)

        try:
            await self.event_stop.wait()
        except Exception as e:
            logger.error("Error in async_tasks: %s", str(e))
            raise e
        finally:
            async_task.cancel()

            try:
                await asyncio.gather(async_task, return_exceptions=True)
            except asyncio.CancelledError:
                logger.info("Asynchronous task cancelled.")

            server_future.cancel()
            consume_future.cancel()

            await asyncio.get_event_loop().run_in_executor(self.thread_pool, server_future.result)
            await asyncio.get_event_loop().run_in_executor(self.thread_pool, consume_future.result)

      
def Consumer_Data_Monitoring(event):
    print("Consumer_1 started")
    consumer_visualizer = ConsumerVisualizer(event)
    consumer_visualizer.Consumer_Data_Monitoring()
    




