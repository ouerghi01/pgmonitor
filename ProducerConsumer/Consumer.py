

import pandas as pd
import matplotlib.pyplot as plt
from dash import Dash, html,dash_table,dcc
from dash.dependencies import Input, Output
import threading
import plotly.express as px
import matplotlib.animation as animation
from kafka import KafkaConsumer
import time
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable

import matplotlib.dates as mdates
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

def animate():
    global activities
    messages = consumer.poll(timeout_ms=1000)

    if messages:
        for tp, msgs in messages.items():
            for message in msgs:
                data = message.value 
                       
               
                row=pd.DataFrame([data])
                global activities
                
                activities = pd.concat([activities, row], ignore_index=True)

   



def Consumer_Data_Monitoring(event_stop):
    app.layout = html.Div(children=[
        html.Div('Hello World'),
        dash_table.DataTable(id='table', page_size=10),
        dcc.Graph(id='histogram')
    ])

    @app.callback(
        Output('table', 'data'),
        Output('histogram', 'figure'),
        Input('interval-component', 'n_intervals')
    )
    def update_data(n_intervals):
        # Update activities here
        animate()  # Call the animation or data update function
        data = activities.to_dict('records')
        fig = px.histogram(activities, x='cpu', y='read', histfunc='avg')
        return data, fig

    # Add an interval component for periodic updates
    app.layout.children.append(
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # in milliseconds
            n_intervals=0
        )
    )
    def run_server():
        app.run_server(debug=False)

    # Run the server in a separate thread
    server_thread = threading.Thread(target=run_server)
    server_thread.start()

    while not event_stop.is_set():
        time.sleep(1)




    

