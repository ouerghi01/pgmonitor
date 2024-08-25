import pandas as pd
import matplotlib.pyplot as plt
from dash  import Dash , dcc , html,dash_table,callback,Output,Input 
import tensorflow.keras.backend as K # type: ignore

from aiokafka import AIOKafkaConsumer
import re 
import asyncio
import os
os.environ['PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT'] = '10'  # Increase timeout to 1 second

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
        self.pg_stat_context_io_queue = Queue()
        self.pg_stat_context_io=pd.DataFrame()
        self.io_operations_context_bar_chart={}
        self.pg_stat_io_activity_queue = Queue()
        self.pie_chart={}
        self.pg_stat_io_activity=pd.DataFrame()
        self.option_default = 'total_size'
        self.parallel_coordinates = {}
        self.system_load_line={}
        try:

           self.consumer =KafkaConsumer("db-monitoring", bootstrap_servers='kafka:9093',auto_offset_reset='earliest',enable_auto_commit=True,value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        except Exception as e:
           print("from consumer 1")
           print(e)
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
            className='sidebar-nav'
        )
        ],
        className="sidebar",
        style={"position": "fixed", "left": 0, "top": 0, "bottom": 0, "width": "250px", "padding": "2rem 1rem", "background-color": "#333"}
         )
        self.app.layout = dbc.Container([
            dbc.Row([
                dbc.Col(sidebar, width=2),
                dbc.Col([
                    dcc.Location(id='url', refresh=False),
                    html.Div(id="page-content"),
                ], width=10),
            ]),
            dcc.Interval(id='interval', interval=10*1000, n_intervals=0)  # Correct placement
        ])
        self.io_operations_bar_chart = {}
        self.data_lock = threading.Lock()
        self.activities_queue = Queue()
        self.pg_stat_user_tables_queue = Queue()
        self.pg_stat_activity_queue=Queue()
        self.event_stop = _event_stop
        self.retrain_interval = 100  # Retrain after every 100 messages
        self.message_count = 0
        self.emailSender=EmailSender()
        self.initialized=False
        self.pipeline=AnomalyDetectionPipeline(columns=self.activities.columns,path='ProducerConsumer/Anomaly_detection_pipeline/models/xprocessor.h5')
        #self.new_pieline=AnomalyDetectionPipeline(columns=self.activities.columns,path='ProducerConsumer/Anomaly_detection_pipeline/models/xprocessor_1.pkl')
    async def setup(self):
        if not self.initialized:

          try:
             self.ai_consumer = AIOKafkaConsumer('query-monitoring', bootstrap_servers='kafka:9093',auto_offset_reset='earliest',enable_auto_commit=True,value_deserializer=lambda x: json.loads(x.decode('utf-8')))
             await self.ai_consumer.start()
          except Exception  as e:
              logger.exception("Error async consuming %s", e)
        self.initialized=True
              
       

    def consume_messages(self):
      time.sleep(10)
      while not self.event_stop.is_set():
        messages = self.consumer.poll(timeout_ms=10000)
        if messages:
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
        func_dict={ "pg_stat_user_tables":self.process_data_pg_stat_user_tables,"pg_stat_context_io":self.process_data_pg_stat_context_io
                   ,
                   "pg_stat_activity":self.process_data_stat_activity,"pg_stat_io_activity":self.process_data_io_operations}
        func_dict[type_query](record)
            #print("shape of pg_stat_user_tables",self.pg_stat_user_tables.shape)
    def process_data_pg_stat_context_io(self,record):
        for item in record:
            new_row = pd.DataFrame([item])
            with self.data_lock:
                self.pg_stat_context_io = pd.concat([self.pg_stat_context_io, new_row], ignore_index=True)
        self.pg_stat_context_io_queue.put(('pg_stat_context_io',self.pg_stat_context_io))
    def process_data_stat_activity(self,record):
        for item in record:
            new_row = pd.DataFrame([item])
            with self.data_lock:
                self.pg_stat_activity = pd.concat([self.pg_stat_activity, new_row], ignore_index=True)
        self.pg_stat_activity_queue.put(('pg_stat_activity',self.pg_stat_activity))
    def process_data_io_operations(self,record):
        for item in record:
            new_row = pd.DataFrame([item])
            self.pg_stat_io_activity = pd.concat([self.pg_stat_io_activity, new_row], ignore_index=True)
        self.pg_stat_io_activity_queue.put(('pg_stat_io_activity',self.pg_stat_io_activity))
            
    def process_data_pg_stat_user_tables(self,record):
        for item in record:
            new_row = pd.DataFrame([item])
            with self.data_lock:
                self.pg_stat_user_tables = pd.concat([self.pg_stat_user_tables, new_row], ignore_index=True)
        self.pg_stat_user_tables_queue.put(('pg_stat_user_tables',self.pg_stat_user_tables))
    def handle_anomaly(self, message):
        data = message.value 
        row=pd.DataFrame([data])
        print(row)
        row_activity = row.copy()
        future=self.thread_pool.submit(self.pipeline.final_prediction,self.transform_row_data(row))
        prediction=future.result()
        queue_pred=Queue()
        queue_pred.put(('prediction',prediction))
        self.thread_pool.submit(self.handle_activity,row_activity,queue_pred)
                     
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

    def handle_activity(self, row_activity, queue_pred):
        prediction = queue_pred.get()[1]
        self.message_count+=1           
        row_activity['predicted_label'] = prediction["predicted_label"]
        self.activities = pd.concat([self.activities, row_activity], ignore_index=True)
        print(self.activities)
        self.activities_queue.put(('activity',self.activities))

                     
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
    def normalize_query(self, x):
        x['query'] = x['query'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x))
        x['query'] = x['query'].apply(lambda x: x.lower())
        x['query'] = x['query'].apply(lambda x: re.sub(r'\s+', ' ', x))
        x['query'] = x['query'].apply(lambda x: x.strip())
        x['query'] = x['query'].apply(lambda x: re.sub(r'\d+', 'NUM', x))
        x['query'] = x['query'].apply(lambda x: re.sub(r'\b\w{1,2}\b', '', x))
        x['query'] = x['query'].apply(lambda x: re.sub(r'\s+', ' ', x))
        x['query'] = x['query'].apply(lambda x: x.strip())

    def convert_activities(self):
        self.activities['cpu']=pd.to_numeric(self.activities['cpu'],downcast='float')
        #self.activities['query'] = self.activities['query'].apply(lambda x : x.replace('$',''))
        #self.activities['query'] = self.activities['query'].apply(lambda x: str(x[:8]) +'...')
        self.activities['duration']=pd.to_numeric(self.activities['duration'],downcast='float')
        self.activities['memory']=pd.to_numeric(self.activities['memory'],downcast='float')
        self.activities['read']=pd.to_numeric(self.activities['read'],downcast='float')
        self.activities['write']=pd.to_numeric(self.activities['write'],downcast='float')
    def run_server(self):
        time.sleep(10)
        @self.app.callback(
            Output("page-content", "children"),
            [Input("url", "pathname"),Input('interval', 'n_intervals') ],
        )
        def render_page_content(n,n1):
            if n == "/Overview":
                return dbc.Container([
    # First Row: Title
    dbc.Row([
        html.H1("Postgres Table Overview", style={
            "textAlign": "center",
            "color": "#212529",
            "fontSize": "2.5rem",
            "margin": "20px 0",
            "fontFamily": "Arial, sans-serif",
            "fontWeight": "bold",
            "backgroundColor": "#f8f9fa",
            "padding": "15px",
            "borderRadius": "5px",
            "boxShadow": "0 2px 4px rgba(0, 0, 0, 0.1)"
        }),
    ]),
    
    # Second Row: Pie Chart and Anomaly Classification
    dbc.Row([
        dbc.Col(
            [
                html.H4(id="category_title", style={
                    "text-align": "center", 
                    "color": "#007bff", 
                    "margin-bottom": "20px"
                }),
                dcc.Graph(
                    id="pie_chart",
                    figure=self.pie_chart,
                    className="graph-container"  # Apply graph-container styling
                ),
                dcc.Dropdown(
                    id="values",
                    options=[
                        {'label': 'Total Size', 'value': 'total_size'},
                        {'label': 'Table Size', 'value': 'table_size'},
                        {'label': 'Indexes Size', 'value': 'indexes_size'}
                    ],
                    value=self.option_default,
                    clearable=False,
                    className="dcc-dropdown"  # Apply dropdown styling
                ),
            ],
            width=6,  # Set width to 6 for balance
        ),
        dbc.Col(
            [
                dcc.Graph(
                    id='anomaly_classification',
                    figure=self.parallel_coordinates,
                    className="graph-container"  # Apply graph-container styling
                )
            ],
            width=6,  # Set width to 6 for balance
        )
    ], className="dash-container"),  # Apply dash-container styling
    
    # Third Row: System Load Line Chart
    dbc.Row([
        dbc.Col(
            [
                dcc.Graph(
                    id='system_load_line',
                    figure=self.system_load_line,
                    className="graph-container"  # Apply graph-container styling
                ),
                dcc.Dropdown(
                    id='load_parameters_y',
                    options=[
                        {'label': 'CPU', 'value': 'cpu'},
                        {'label': 'Memory', 'value': 'memory'},
                        {'label': 'Read', 'value': 'read'},
                        {'label': 'Write', 'value': 'write'}
                    ],
                    value='cpu',
                    className="dcc-dropdown",  # Apply dropdown styling
                    style={"margin-top": "10px"}  # Add margin-top for spacing
                )
            ],
            width=12,  # Full width for the system load graph
        )
    ], className="dash-container"),
    
], fluid=True)

            elif n == "/Query_Performance":
                activities=self.activities_queue.get()[1]
                activities['duration']=pd.to_numeric(activities['duration'],downcast='float')
                query_execution_time_distribution=activities.groupby(['query','datetimeutc'],as_index=False).agg({'duration':'mean'}).reset_index()
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
                return dbc.Container(
                    [
                        html.H1(
                            "IO Statistics Statistics PostgreSQL",
                            style={
                                "textAlign": "center",           # Center the text
                                "color": "#333",                 # Dark gray color for the text
                                "fontSize": "2.5rem",            # Responsive font size
                                "margin": "20px 0",              # Margin above and below the heading
                                "fontFamily": "Arial, sans-serif", # Font family for a clean look
                                "fontWeight": "bold",            # Bold text
                                "backgroundColor": "#f8f9fa",    # Light gray background
                                "padding": "10px",               # Padding around the text
                            }),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dcc.Graph(
                                            id='io-operations-bar-chart',
                                            figure=self.io_operations_bar_chart
                                        )
                                    ],
                                    width=6
                                ),
                                dbc.Col(
                                    [
                                        dcc.Graph(
                                            id='io-operations-context-bar-chart',
                                            figure=self.io_operations_context_bar_chart
                                        )
                                    ],
                                    width=6
                                ),
                                
                            ]
                        )
                    ]
                )

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
            [Input("url", "pathname"),Input("time-slider", "value"),Input("interval",'n_intervals')],
        )
        def update_query_performance(n, selected_range,n1):
            if n == "/Query_Performance":
                activities=self.activities_queue.get()[1]
                # Convert `datetimeutc` to timezone-naive datetime
                activities['datetimeutc'] = pd.to_datetime(activities['datetimeutc']).dt.tz_localize(None)
                start_date = pd.to_datetime(selected_range[0], unit='s').tz_localize(None)
                end_date = pd.to_datetime(selected_range[1], unit='s').tz_localize(None)
                activities['query']=activities['query'].apply(lambda x:str(x)[:10])
                if(activities.shape[0] > 0):
                       for column in ['cpu', 'memory', 'read', 'write']:
                            activities[column] = pd.to_numeric(activities[column], errors='coerce', downcast='float')
                       for column in ['cpu', 'memory', 'write', 'read']:
                            mean_value = activities[column].mean()
                            activities[column].fillna(mean_value, inplace=True)
                else:
                    raise ValueError
                activities['duration']=pd.to_numeric(activities['duration'],downcast='float')
                top_10_queries = activities.groupby('query').agg({'cpu': 'mean','write':'mean','read':'mean','memory':'sum'}).sort_values('cpu', ascending=False).head(10)
                query_execution_time_distribution=activities.groupby(['query','datetimeutc'],as_index=False).agg({'duration':'mean'}).reset_index()
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
        @self.app.callback(
            Output("io-operations-bar-chart", "figure"),
            Output("io-operations-context-bar-chart", "figure"),
            [Input("url", "pathname"),Input('interval', 'n_intervals')],
        )
        def update_io_operations_bar_chart(n,n1):
            if n == "/IO_Statistics":
                try:

                   pg_stat_io=self.pg_stat_io_activity_queue.get()[1]
                   pg_stat_io_context=self.pg_stat_context_io_queue.get()[1]
                   df_melted = pd.melt(pg_stat_io_context, id_vars=['context'], 
                    value_vars=['total_reads', 'total_writes', 'total_writebacks'],
                    var_name='operation', value_name='total_operations')
                   pg_stat_io.dropna()
                   pg_stat_io_context.dropna()
                   fig_io_operations_bar_chart = px.bar(pg_stat_io, x='backend_type', y='total_io_operations', title='I/O Operations')
                   fig_io_operations_context_bar_chart = px.bar(df_melted, x='context', y='total_operations', color='operation', 
                             title="Distribution of I/O Operations by Context",
                             labels={'context': 'I/O Context', 'total_operations': 'Total I/O Operations'},
                             barmode='group')
                   self.io_operations_context_bar_chart=fig_io_operations_context_bar_chart
                   self.io_operations_bar_chart=fig_io_operations_bar_chart
                   return fig_io_operations_bar_chart
                except Exception as e:
                   logger.error(f"Error in updating IO Operations Bar Chart: {e}")
                   return {}
            
            return {}
        @self.app.callback(
            Output("activities_table", "data"),
            Output("area_chart", "figure"),
            Output("line_chart","figure"),
            Output("queries_executed_by_different_users","figure"),
            Output("stacked_bar_chart","figure"),
            [Input("url", "pathname"),Input('interval', 'n_intervals')],
        )
        def update_activities_table(n,n1):
            if n == "/Activity_Monitoring":
                pg_stat_activity=self.pg_stat_activity_queue.get()[1]
                activities=self.activities_queue.get()[1]
                pg_stat_activity.dropna()
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
                currently_active_queries=activities[activities['state']=='active'][['query','state','duration']]
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
            [Input("url","pathname"), Input("values", "value"), Input("load_parameters_y", "value"),Input("interval",'n_intervals')],
        )
        def update_overview_chart(n,value,value_y,n1):
            try:
                self.option_default = value
                activities=self.activities_queue.get()[1]
                pg_user_tables=self.pg_stat_user_tables_queue.get()[1]
                if (activities.shape[1]>0 and pg_user_tables.shape[1]>0) :
                    print("ok\n")
                else :
                    raise Exception
                if n != "/Overview":
                    return {}, "",{}
                elif value and value_y is None:
                    return {}, "Table Overview",{}
                
                try:
                    data_operations =None

                    def size_to_float(size_str):
                        if size_str is None or size_str == '':
                            return 0.0
                        size_str = size_str.upper().strip()
                        if size_str.endswith('MB'):
                            return float(size_str.replace('MB', '').strip())
                        elif size_str.endswith('KB'):
                            return float(size_str.replace('KB', '').strip()) / 1024
                        elif size_str.endswith('GB'):
                            return float(size_str.replace('GB', '').strip()) * 1024
                        elif size_str.endswith('BYTES'):
                            return float(size_str.replace('BYTES', '').strip()) / (1024 ** 2)  # Convert bytes to MB
                        else:
                            try:
                                return float(size_str.strip())  # Try to convert directly if no unit is specified
                            except ValueError:
                                return 0.0  # Return 0.0 for invalid values
                    if(activities.shape[0] > 0):
                       for column in ['cpu', 'memory', 'read', 'write']:
                            activities[column] = pd.to_numeric(activities[column], errors='coerce', downcast='float')
                       for column in ['cpu', 'memory', 'write', 'read']:
                            mean_value = activities[column].mean()
                            activities[column].fillna(mean_value, inplace=True)
                    else:
                        raise ValueError
                   
                    activities.dropna()
                    activities.dropna(subset=['cpu', 'memory', 'read', 'write','query'], inplace=True)
                    if pg_user_tables.shape[0] ==0:
                        raise ValueError
                    data_by_table = pg_user_tables[['table_name','schemaname', self.option_default]].copy()
                    data_by_table = data_by_table.sort_values(by=self.option_default, ascending=False)
                    data_by_table[self.option_default] = data_by_table[self.option_default].apply(lambda x: size_to_float(x))
                    if data_by_table.shape[0] ==0:
                        raise ValueError
                    activities['duration']=pd.to_numeric(activities['duration'], downcast='float')
                    activities['query']=activities['query'].apply(lambda x:str(x)[0:10])
                    activities['info']='Duration: '+activities['duration'].astype(str)+'s, Wait: '+activities['wait'].astype(str)+'s'
                    try:
                        data_operations = activities.groupby(['query','info','predicted_label'],as_index=False).agg({'cpu': 'mean', 'memory': 'mean', 'write': 'mean', 'read': 'mean'}).reset_index().head(10)
                    except Exception as e:
                        print(e)
                        raise e
                except Exception as e:
                    print(e)
                    raise e
                   
                threshold_size = 200
                exceeding_tables = data_by_table[data_by_table[self.option_default] > threshold_size]['table_name'].tolist()
                
                pie_chart_figure = px.pie(
                    data_by_table,
                    values=self.option_default,
                    names='table_name',
                    width=500,  # Increased width
                    height=300,  # Increased height
                    title="Table Overview by Size",
                    color_discrete_map={
                        table: 'red' if table in exceeding_tables else 'blue'
                        for table in data_by_table['table_name']
                    }
                )
                
                for index, table in enumerate(exceeding_tables):
                    row = data_by_table[data_by_table['table_name'] == table].iloc[0]
                    size = row[self.option_default]
                    percentage = size / data_by_table[self.option_default].sum() * 100
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
                # Get a list of unique labels
                unique_labels = data_operations['predicted_label'].unique()
                # Create a color map for the labels
                # Generate a color for each unique label using a colormap
                colors = plt.cm.get_cmap('tab20', len(unique_labels)).colors
                # Create a dictionary mapping each label to a color
                label_color_mapping = {label: f"rgb{tuple(map(int, tuple(c * 255 for c in colors[idx])))}" for idx, label in enumerate(unique_labels)}
                # Apply the color mapping
                data_operations['label_color'] = data_operations['predicted_label'].map(label_color_mapping)

                parallel_coordinates = px.parallel_coordinates(
                data_operations,
                dimensions=['cpu', 'memory', 'read', 'write'],
                color='label_color',
                labels={'cpu': 'CPU', 'memory': 'Memory', 'read': 'Read', 'write': 'Write'},
                color_continuous_scale=px.colors.diverging.Tealrose_r,  # Custom color scale for better visualization
                title="Parallel Coordinates Plot of Metrics with Anomaly Detection"
                )
            
            # Update the traces to add hover information
                for label in unique_labels:
                    trace_indices = data_operations[data_operations['predicted_label'] == label].index
                    hover_text = data_operations.loc[trace_indices, 'query'].tolist()
                    parallel_coordinates.update_traces(
                        selector={'line': {'color': label_color_mapping[label]}},
                        hoverinfo='text',
                        hovertext=hover_text
                    )
                self.parallel_coordinates=parallel_coordinates
                self.pie_chart=pie_chart_figure
                
                fig_system_load = px.line(
                    data_operations,
                    x='cpu',
                    y=value_y,
                    color='query',
                    hover_data={'info':True},  # Hover to show details
                    title='Operations Load',
                    markers=True,
                    width=800,  
                    height=500   
                )
                self.system_load_line=fig_system_load
                
                pie_chart_figure.update_layout(autosize=True, width=500,height=300, font=dict(size=12),margin=dict(t=50, l=25, r=25, b=25))
                fig_system_load.update_traces(textposition='top center')
                fig_system_load.update_layout(hovermode='closest')

                return pie_chart_figure, f"Table  by {value}",fig_system_load,
            
            except Exception as e:
                print(e)
                raise e
        with self.data_lock :

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
    




