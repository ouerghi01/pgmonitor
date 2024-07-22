import pandas as pd
import matplotlib.pyplot as plt
from dash import Dash, html,dash_table,dcc
from dash.dependencies import Input, Output
import threading
import plotly.express as px
from kafka import KafkaConsumer
import time

import threading

import json
import numpy as np
# Assuming you have set up your Kafka consumer

app=Dash()
consumer = KafkaConsumer(
    'db-monitoring',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

activities=pd.DataFrame()
data_lock = threading.Lock()
def remove(query):
    return query.replace('$$','')
def consume_messages(event_stop):
    global activities
    messages = consumer.poll(timeout_ms=1000)
    while not event_stop.is_set():
        with data_lock:
           if messages:
              for tp, msgs in messages.items():
                  for message in msgs:
                     data = message.value 
                     row=pd.DataFrame([data])                
                     activities = pd.concat([activities, row], ignore_index=True)
                     activities['query'] = activities['query'].apply(remove)
                     activities['query'] = activities['query'].apply(lambda x: x[:12])
    time.sleep(1)

   



def Consumer_Data_Monitoring(event_stop):
    consumer_thread=threading.Thread(target=consume_messages,args=(event_stop,))
    consumer_thread.start()
    app.layout = html.Div(children=[
        html.Div('Real Time Data Monitoring', style={'fontSize': 24, 'textAlign': 'center', 'marginBottom': 20,"color":"blue"}),
        dash_table.DataTable(id='table', page_size=10),
        dcc.Graph(id='histogram'),
        html.P(id='Standard Deviation', style={'fontSize': 18, 'textAlign': 'center', 'marginTop': 20}) ,
        
        dcc.Graph(id='Pie')
        
    ])

    @app.callback(
        Output('table', 'data'),
        Output('histogram', 'figure'),
        Output('Pie', 'figure'),
        Input('interval-component', 'n_intervals'),
    )
    def update_data(n_intervals):
        global activities
       

        with data_lock:
        # Ensure 'mean' is a valid integer index
            if activities is not None:
                data = activities.to_dict('records')
                fig = px.histogram(activities, x='cpu', y='write', histfunc='avg', nbins=20)
                fig.update_layout(bargap=0.1, title="CPU vs Read Histogram")
                pie=px.pie(activities, values='memory', names='query' , labels={'cpu':'CPU Utilization','query':'Query'},title='CPU Utilization by Query')
                return data, fig ,pie

    # Add an interval component for periodic updates
    app.layout.children.append(
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # in milliseconds
            n_intervals=0,
            max_intervals=-1,
        )
    )
    def run_server():
        time.sleep(2)
        app.run_server(debug=False)

    # Run the server in a separate thread
    server_thread = threading.Thread(target=run_server)
    server_thread.start()

    while not event_stop.is_set():
        time.sleep(1)
    try:
        while not event_stop.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        event_stop.set()
    server_thread.join()
    



