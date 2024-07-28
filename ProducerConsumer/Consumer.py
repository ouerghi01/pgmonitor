import pandas as pd
import matplotlib.pyplot as plt
from dash  import Dash , dcc , html,dash_table,callback,Output,Input
from sklearn.ensemble import IsolationForest
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import dash_bootstrap_components as dbc
import threading
import plotly.express as px
from kafka import KafkaConsumer
from joblib import load, dump
import time
from queue import Queue 
import threading
import os 
import json
import numpy as np
from ProducerConsumer.Notify import EmailSender
class ConsumerVisualizer:
    def __init__(self):
        self.consumer =KafkaConsumer('db-monitoring', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',enable_auto_commit=True,value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.activities = pd.DataFrame(columns=['datetimeutc', 'pid', 'database', 'appname', 'user', 'client', 'cpu', 'memory', 'read', 'write', 'duration', 'wait', 'io_wait', 'state', 'query'])
        # Load the models
        self.load_models()
        self.app = Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.app.layout=dbc.Container([
        dbc.Row(
        dbc.Col(
            [
                html.H1('Real Time Data Visualization', style={'textAlign': 'center', 'color': 'red', 'fontSize': 30}),
                html.Hr(),
                dash_table.DataTable(
                    id='table',
                    page_size=3,
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
                    }
                )
            ]
        )
    ),
    dbc.Row(
        [
            dbc.Col(
                [
                    html.H4('Memory Usage Over Time', style={'textAlign': 'center'}),
                    dcc.Graph(id='line-chart-memory', figure={})
                ],
                width=4
            ),
            dbc.Col(
                [
                    html.H4('CPU Usage Over Time', style={'textAlign': 'center'}),
                    dcc.Graph(id='line-chart-cpu', figure={})
                ],
                width=4
            ),
            dbc.Col(
                [
                    html.H4('Duration Over Time', style={'textAlign': 'center'}),
                    dcc.Graph(id='line-duration-over-time', figure={})
                ],
                width=4
            )
        ]
    ),
    dbc.Row(
        [
            dbc.Col(
                [
                    html.H4('Read Operations Over Time', style={'textAlign': 'center'}),
                    dcc.Graph(id='line-read-over-time', figure={})
                ],
                width=6
            ),
            dbc.Col(
                [
                    html.H4('Write Operations Over Time', style={'textAlign': 'center'}),
                    dcc.Graph(id='line-write-over-time', figure={})
                ],
                width=6
            )
        ]
    ),
    dbc.Row(
        dbc.Col(
            [
                html.H4('Operations Distribution', style={'textAlign': 'center'}),
                dcc.Graph(id='pie-chart', figure={})
            ],
            width=12
        )
    ),
    dbc.Row(
        dbc.Col([
            html.H4('Top Longest Running Queries', style={'textAlign': 'center'}),
            dash_table.DataTable(
                id='longest-running-queries',
                page_size=5,
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
                }
            )
        ],
        width=12)
    ),
    dcc.Interval(id='interval', interval=5*1000, n_intervals=0, max_intervals=-1)
])
        self.data_lock = threading.Lock()
        self.out_q = Queue()
        self.event_stop = threading.Event()
        self.retrain_interval = 100  # Retrain after every 100 messages
        self.message_count = 0
        self.emailSender=EmailSender()

    def load_models(self):
        self.scaler_numeric=load( os.path.abspath( '/home/aziz/DBWatch/Stage_Bri/ProducerConsumer/models/scaler.pkl'))
        self.scaler_categorical=load(os.path.abspath('/home/aziz/DBWatch/Stage_Bri/ProducerConsumer/models/encoder.pkl'))
        self.tf_idf_vect=load(os.path.abspath('/home/aziz/DBWatch/Stage_Bri/ProducerConsumer/models/tfidf_vect.pkl'))
        self.isolation_forest=load(os.path.abspath('/home/aziz/DBWatch/Stage_Bri/ProducerConsumer/models/isolation_forest.joblib'))
    def consume_messages(self):
      while not self.event_stop.is_set():
        messages = self.consumer.poll(timeout_ms=1000)
        if messages:
            with self.data_lock:
              for _, msgs in messages.items():
                  for message in msgs:
                     data = message.value 
                     row=pd.DataFrame([data])
                     row_activity = row.copy()
                     Data = self.prepare_data_for_prediction(row)
                     prediction = self.isolation_forest.predict(Data.values)
                     self.message_count+=1
                     if prediction[0] == -1:
                         print('Anomaly detected')
                         self.emailSender.sendEmail({
                            "Anomaly":-1
                         })
                     else:
                         print('Anomaly not detected')
                     
                     self.activities = pd.concat([self.activities, row_activity], ignore_index=True)
                     if self.message_count % self.retrain_interval == 0:
                          thread_retain=threading.Thread(target=self.retrain_models)
                          thread_retain.start()
                     self.convert_activities()
      time.sleep(1)
    def retrain_models(self):
        print("training start\n")

        with self.data_lock:
           
          Dataset=self.activities.copy()
        isolation_forest = IsolationForest(n_estimators=100, contamination='auto')
        Data=self.prepare_data_for_prediction(Dataset)
        isolation_forest.fit(Data.values)
        dump(isolation_forest, os.path.abspath('/home/aziz/DBWatch/Stage_Bri/ProducerConsumer/models/isolation_forest.joblib'))
        self.isolation_forest = isolation_forest
    
       
    def prepare_data_for_prediction(self, row):
        self.transform_row_data(row)
        fixed_numeric = ['pid','cpu', 'memory', 'read', 'write', 'duration']
        numeric_features = list(row.select_dtypes(include=['int64', 'float64']).columns)
        for feature in fixed_numeric:
           if feature not in numeric_features:
              row[feature] = 0 
        
        numeric_features.extend(['year', 'month', 'day', 'hour', 'minute', 'second', 'day_of_week','is_weekend'])
        row['is_weekend'] = (row['datetimeutc'].dt.dayofweek >= 5).astype(np.int64)
        row[numeric_features] = self.scaler_numeric.transform(row[numeric_features])
        row.drop(['datetimeutc','pid'], axis=1, inplace=True)
        categorical_features = ['state', 'user', 'wait', 'io_wait', 'client', 'appname', 'database']
        row[categorical_features] = row[categorical_features].fillna('unknown')
        new_data_encoded = self.scaler_categorical.transform(row[categorical_features])
        encoded_df = pd.DataFrame(new_data_encoded, columns=self.scaler_categorical.get_feature_names_out(categorical_features))
        Data = pd.concat([row, encoded_df], axis=1)
        Data.drop(columns=categorical_features, inplace=True)
        new_data_query_vectors = self.tf_idf_vect.transform(row['query'])
        query_vectors_df = pd.DataFrame(new_data_query_vectors.toarray(), columns=self.tf_idf_vect.get_feature_names_out())
        Data = pd.concat([Data, query_vectors_df], axis=1)
        Data.drop(columns=['query'], inplace=True)
        return Data

    def transform_row_data(self, row):
        row['cpu'] = pd.to_numeric(row['cpu'], errors='coerce')
        row['memory'] = pd.to_numeric(row['memory'], errors='coerce')
        row['read'] = pd.to_numeric(row['read'], errors='coerce')
        row['write'] = pd.to_numeric(row['write'], errors='coerce')
        row['duration'] = pd.to_numeric(row['duration'], errors='coerce') 
        row['datetimeutc'] = pd.to_datetime(row['datetimeutc'], errors='coerce') 
        row['year'] = row['datetimeutc'].dt.year
        row['month'] = row['datetimeutc'].dt.month
        row['day'] = row['datetimeutc'].dt.day
        row['hour'] = row['datetimeutc'].dt.hour
        row['minute'] = row['datetimeutc'].dt.minute
        row['second'] = row['datetimeutc'].dt.second
        row['day_of_week'] = row['datetimeutc'].dt.dayofweek
        

        row['pid']=pd.to_numeric(row['pid'], errors='coerce')

    def convert_activities(self):
        self.activities['cpu']=pd.to_numeric(self.activities['cpu'],downcast='float')
        self.activities['query'] = self.activities['query'].apply(lambda x : x.replace('$',''))
        self.activities['query'] = self.activities['query'].apply(lambda x: x[:12])
        self.activities['duration']=pd.to_numeric(self.activities['duration'],downcast='float')
        self.activities['memory']=pd.to_numeric(self.activities['memory'],downcast='float')
        self.activities['read']=pd.to_numeric(self.activities['read'],downcast='float')
        self.activities['write']=pd.to_numeric(self.activities['write'],downcast='float')
    def run_server(self):
         @self.app.callback(Output('table', 'data'),Output('pie-chart', 'figure'),Output('line-chart-cpu', 'figure'),Output('line-chart-memory', 'figure'),Output('line-read-over-time', 'figure'),Output('line-write-over-time', 'figure'),Output('line-duration-over-time', 'figure'),Output('longest-running-queries', 'data'),Input('interval', 'n_intervals'))  
         def update_table(n_intervals):
            with self.data_lock:
              if self.activities.empty:
                   return [], {}, {}, {}, {}, {}, {}, []
              Data = self.activities.to_dict('records')
              query_by_cpu = self.activities.groupby('query')['cpu'].mean().reset_index()
              longest_running_queries = self.activities.nlargest(10, 'duration').to_dict('records')

              pie_chart = px.pie(query_by_cpu, values='cpu', names='query', title='CPU Usage by Query')
              line_chart_cpu = px.line(self.activities, x='datetimeutc', y='cpu', color='query', title='CPU Usage Over Time')
              line_chart_memory = px.line(self.activities, x='datetimeutc', y='memory', color='query', title='Memory Usage Over Time')
              line_chart_duration = px.line(self.activities, x='datetimeutc', y='duration', color='query', title='Duration Over Time')
              line_chart_read = px.line(self.activities, x='datetimeutc', y='read', color='query', title='Read Operations Over Time')
              line_chart_write = px.line(self.activities, x='datetimeutc', y='write', color='query', title='Write Operations Over Time')
              return Data, pie_chart, line_chart_cpu, line_chart_memory, line_chart_read, line_chart_write, line_chart_duration, longest_running_queries
         time.sleep(5)
         self.app.run_server(debug=False)
    def Consumer_Data_Monitoring(self):
      consumer_thread=threading.Thread(target=self.consume_messages)
      consumer_thread.start()
      server_thread = threading.Thread(target=self.run_server)
      server_thread.start()
      try:
        while not self.event_stop.is_set():
            time.sleep(1)
      except KeyboardInterrupt:
        self.event_stop.set()
       # server_thread.join()
        consumer_thread.join()
def Consumer_Data_Monitoring():
    consumer_visualizer = ConsumerVisualizer()
    consumer_visualizer.Consumer_Data_Monitoring()
    




