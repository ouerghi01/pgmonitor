from dash import Dash, html, dcc ,dash_table
import plotly.express as px
import dash_mantine_components as dmc #type: ignore
from dash_iconify import DashIconify #type: ignore
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
class PostgresDashboard:
    def __init__(self):
        self.data = pd.read_csv('Data/pg_stat_user_tables.csv')
        self.activities=pd.read_csv('Data/activities.csv',sep=';')
        self.pg_stat_activity=pd.read_csv('Data/pg_stat_activity.csv',sep=',')
    
        self.app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions=True)
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
            ])
        ])
    
    def run_server(self):
        @self.app.callback(
            Output("page-content", "children"),
            [Input("url", "pathname")],
        )
        def render_page_content(n):
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
                self.activities['query']=self.activities['query'].apply(lambda x:x[:12])
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
            [Input("url", "pathname"),Input("interval", "n_intervals")],
        )
        def update_activities_table(n,n1):
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
            if n != "/Overview":
                return {}, "",{}
            elif value and value_y is None:
                return {}, "Table Overview",{}
            try:
                data_by_table = self.data[['relname','schemaname', value]].copy()
                data_by_table[value] = data_by_table[value].apply(lambda x: float(x.replace(' MB', '').strip()))
                self.activities['query']=self.activities['query'].apply(lambda x:x[:12])
                self.activities['info']='Duration: '+self.activities['duration'].astype(str)+'s, Wait: '+self.activities['wait'].astype(str)+'s'
                data_operations = self.activities.groupby(['query','info'],as_index=False).agg({'cpu': 'mean', 'memory': 'mean', 'write': 'mean', 'read': 'mean'}).reset_index().head(10)
                threshold_size = 200
                exceeding_tables = data_by_table[data_by_table[value] > threshold_size]['relname'].tolist()
                pie_chart_figure = px.pie(
                    data_by_table,
                    values=value,
                    names='relname',
                    title="Table Overview by Size",
                    color_discrete_map={
                        table: 'red' if table in exceeding_tables else 'blue'
                        for table in data_by_table['relname']
                        }
                )
                for index, table in enumerate(exceeding_tables):
                    row = data_by_table[data_by_table['relname'] == table].iloc[0]
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
                        showarrow=True,font=dict(size=14, color='black'),
                        arrowhead=1,
                        ax=0,
                        ay=-40,
                        hovertext=f"{row['relname']} exceeds {threshold_size}MB ({percentage:.1f}%)",  # Tooltip text on hover
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
                return {}, "Table Overview",{}
            
        self.app.run_server(debug=True)
