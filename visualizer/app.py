from model_dataclass import *
import json
from types import SimpleNamespace
import tkinter as tk
#import simulation as sim
import pandas as pd
from collections import defaultdict
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table, callback,State

#DISK_RESOURCE=0
app = Dash(__name__)

def load_data(time_step):
    #print(time_step)
    reader = SimulationReader("test", "2022-03-25_17.11.40")
    step = reader.getSimulationStep(time_step)
    #print(step)
    return step


def get_data(data):
    #print(data)
    node_shard_allocate_list=defaultdict(list)
    for x,y in data.ClusterState.CurrentAssignment.items():
        for z in y:
            node_shard_allocate_list[z].append(x)
            #print("Shard:",x,"Is Stored on Node:",z)
    #print(node_shard_allocate_list[0])

    return node_shard_allocate_list

def create_shard_table(app,data):
    QPS_Set=False
    if '1' in data.ClusterState.Shards[0].Demands:
        QPS_Set=True
    params = [
    'Shard-ID', 'RF','Tags', 'Capacity Required', 'QPS',
    'Assigned on Nodes'
    ]
    app.layout = html.Div([
        dash_table.DataTable(
            id='Shard-Table',
            columns=(
                [{'id': p, 'name': p} for p in params]
            ),
            data=[
            {
                "Shard-ID":i,
                "RF": data.Configuration.Rf,
                "Tags": data.ClusterState.Shards[i].Tags,
                "Capacity Required": data.ClusterState.Shards[j].Demands['0'],
                "QPS": data.ClusterState.Shards[j].Demands['1'] if QPS_Set else 'Not Set',
                "Assigned on Nodes": ','.join(map(str, data.ClusterState.CurrentAssignment[i]))
            }
            for i in sorted(data.ClusterState.Shards)
            ],
            editable=False
        ),
    ])
    app.run_server(debug=True)

def create_node_table(data,node_shard_list):
    #print("Here")
    QPS_Set=False
    if '1' in data.ClusterState.Nodes[0].Resources:
        QPS_Set=True
    params = [
    'Node-ID','Tags', 'Max Capacity', 'QPS',
    'Assigned Shards'
    ]

    fig = go.Figure(data=[go.Table(
    header=dict(values=params,
                line_color='darkslategray',
                fill_color='lightskyblue',
                align='left'),
    cells=dict(values=[ {
                "Node-ID":i,
                "Tags": data.ClusterState.Nodes[i].Tags,
                "Max Capacity": data.ClusterState.Nodes[i].Resources['0'],
                "QPS": data.ClusterState.Nodes[i].Resources['1'] if QPS_Set else 'Not Set',
                "Assigned Shards": ','.join(map(str, sorted(node_shard_list[i]))) if node_shard_list[i] else 'No Shards Assigned'
            }
            for i in sorted(data.ClusterState.Nodes)
            ],
               line_color='darkslategray',
               fill_color='lightcyan',
               align='left'))
    ])

    fig.update_layout(width=500, height=300)
    return fig

url_bar_and_content_div = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

index_page = html.Div([
    dcc.Link('Current Assignments', href='/currentassignments'),
    html.Br(),
    dcc.Link('Node Table', href='/nodetable'),
    html.Br(),
    dcc.Link('Shard Table', href='/shardtable'),
])

currentassignments_layout = html.Div([
    html.Div([
        #html.Div(id='page-1-content'),∂
        html.H1('Current Assignments'),
        html.H3 ('Allocation Options: Capcity, Disk Resource Balancing , Unique Replicas on Node, QPS Balancing'),
        html.P("Select to see Node Capacity or QPS in the Allocation:"),
        dcc.Dropdown(
            id="dropdown",
            options=['Capacity', 'QPS'],
            value='Capacity',
            clearable=False,
        ),
    ]),
    dcc.Graph(id="page-1-content"),
    dcc.Slider(
        0,5,1,
        value=0,
        id='timeslider'
    ),
    #html.Div(id='page-1-content'),

])

# index layout
app.layout = url_bar_and_content_div

# "complete" layout
app.validation_layout = html.Div([
    url_bar_and_content_div,
    currentassignments_layout,
])

# Update the index
@callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/currentassignments':
        return currentassignments_layout
    elif pathname=="/nodetable":
        return nodetable_layout
    else:
        return index_page

@app.callback(
        Output('page-1-content', 'figure'),
        Input('timeslider','value'),
        Input('dropdown', 'value'))
def page_1_dropdown(timevalue,option):
    #print(timevalue)
    #print(option)
    file_data=load_data(timevalue)
    node_shard_list=get_data(file_data)
    node_list=[]
    #shard
    for i in file_data.ClusterState.Nodes.keys():
        node_list.append("Node"+str(i))

    capacity_Used=[0]*len(file_data.ClusterState.Nodes.keys())
    QPS_Node=[0]*len(file_data.ClusterState.Nodes.keys())
    QPS_Set=False
    Cap_Set=False
    if '1' in file_data.ClusterState.Shards[0].Demands:
        QPS_Set=True
    if '0' in file_data.ClusterState.Shards[0].Demands:
        Cap_Set=True    
 
    for i in file_data.ClusterState.Nodes.keys():
        #print(i)
        for j in node_shard_list[i]:
            #print(j)
            if Cap_Set:
                capacity_Used[i]=capacity_Used[i]+file_data.ClusterState.Shards[j].Demands['0']
            if QPS_Set:
                QPS_Node[i]=QPS_Node[i]+file_data.ClusterState.Shards[j].Demands['1']
        if Cap_Set: 
            capacity_Used[i]=capacity_Used[i]/file_data.ClusterState.Nodes[i].Resources['0'] *100

    data_to_use=capacity_Used
    if (option == 'QPS'):
        data_to_use=QPS_Node
    fig = go.Figure(
        data=go.Bar(x=node_list,y=data_to_use, # replace with your own data source
                marker_color='purple'))
    fig.update_xaxes(title_text="Nodes")
    if (option == 'QPS'):
        fig.update_yaxes(title_text="QPS")
    else:
        fig.update_yaxes(title_text="Capacity Used")
    return fig

nodetable_layout = html.Div([
     dcc.Graph(id="node-time-slider"),
    dcc.RangeSlider(0, 5, 1, value=0, id='time-range-slider'),
    html.Div(id='node-time-slider')
])
@app.callback(
Output('node-time-slider', 'figure'),
        Input('time-range-slider', 'value'))
def page2_layout_node(timevalue):
    #print(timevalue)
    file_data=load_data(timevalue)
    node_shard_list=get_data(file_data)
    return create_node_table(file_data,node_shard_list)

'''def layout_tabs(app):
    app.layout = html.Div([
        html.H2('Declerative Cluster Management',style='center'),
        html.P("Select Time Interval:"),
        dcc.Dropdown(
            id="time-dropdown",
            options=['0', '1','2','3','4','5'],
            value='0',
            clearable=False,
        ),
        dcc.Dropdown(
            id="option-dropdown",
            options=['CurrentAssignment', 'Nodes','Shards'],
            value='0',
            clearable=False,
        ),

        dcc.Graph(id="time"),
    ]) '''



if __name__=='__main__':
    app.run_server(debug=True)
