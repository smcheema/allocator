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

node_params = [
    'Node-ID','Tags','Capacity Used','Max Capacity','QPS on Node','Max QPS',
    'Assigned Shards'
    ]
allocator_options = [
    'WithCapacity',
    'WithLoadBalancing',
    'WithTagAffinity',
    'WithMinimalChurn',
    'MaxChurn',
    'SearchTimeout',
    'VerboseLogging',
    'Rf'
]
shard_params =['Shard-ID','Tags', 'Capacity Required', 'QPS',
    'Assigned on Nodes']

def load_data(time_step):
    #print(time_step)
    reader = SimulationReader("ali", "newagain")
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

url_bar_and_content_div = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

index_page = html.Div([
    html.Br(),
    html.H1("Declarative Cluster Managment",style = {'textAlign' : 'center'}),
    html.P("Click Here to View Current Asignments"),
    html.A(html.Button('Current Assignments'),href='/currentassignments'),
    #dcc.Link('Current Assignments', href='/currentassignments'),
    html.Br(),
    html.Br(),
    html.P("Click Here to View  Node Table"),
    html.A(html.Button('Node Table'),href='/nodetable'),
    #dcc.Link('Node Table', href='/nodetable'),
    html.Br(),
    html.Br(),
    html.P("Click Here to View  Shard Table"),
    html.A(html.Button('Shard Table'),href='/shardtable'),
])

currentassignments_layout = html.Div([
    html.Div([
        #html.Div(id='page-1-content'),∂
        html.H1('Current Assignments'),
        html.P("Select to see Node Capacity or QPS in the Allocation:"),
        dcc.Dropdown(
            id="dropdown",
            options=['Capacity', 'QPS'],
            value='Capacity',
            clearable=False,
        ),
    ],style={'width': '100%', 'display': 'inline-block'}),
    
    html.Div(children=[
        dcc.Graph(id="current-assignment-content", style={'width':'1000px','margin-left':'10px','flex-grow':'4'}),

    dash_table.DataTable(
        id='allocator-options',
        columns= ([{'id': 'Allocator Options', 'name': 'Allocator Options'},{'id': 'Value', 'name': 'Value'}]),
        data=[],
        editable=False,
        style_table={'width':'200px','height':'400','margin-left':'100px','padding':'100px','text-align':'center','flex':'1'}
    ),
    #html.Div(id='page-1-content'),

], style={'width': '80%','height':'450px' ,'display': 'flex','flex-direction': 'row'}),

html.Br(),
dcc.Slider(
        0,5,1,
        value=0,
        id='timeslider'
    ),

], style={'width': '100%', 'display': 'inline-block'})

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
    elif pathname== "/shardtable":
        return shardtable_layout
    else:
        return index_page

@app.callback(
        Output('current-assignment-content', 'figure'),
        Output('allocator-options', 'data'),
        Input('timeslider','value'),
        Input('dropdown', 'value'))
def current_assignment_dropdown(timevalue,option):
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
        fig.update_yaxes(title_text="Capacity Used (%)")
    data=[{'Allocator Options':'WithCapacity','Value':"Enabled" if file_data.Configuration.WithCapacity else "Disabled"},
    {'Allocator Options':'WithLoadBalancing','Value':"Enabled" if file_data.Configuration.WithLoadBalancing else "Disabled"},
    {'Allocator Options':'WithTagAffinity','Value':"Enabled" if file_data.Configuration.WithTagAffinity else "Disabled"},
    {'Allocator Options':'MinimizeChurn','Value':"Enabled" if file_data.Configuration.WithMinimalChurn else "Disabled"},
    {'Allocator Options':'MaxChurn','Value':file_data.Configuration.MaxChurn},
    {'Allocator Options':'Timeout (ms)','Value':file_data.Configuration.SearchTimeout},
    {'Allocator Options':'Rf','Value':file_data.Configuration.Rf},
    {'Allocator Options':'Logging','Value':"Enabled" if file_data.Configuration.VerboseLogging else "Disabled"},
    {'Allocator Options': 'Time taken (ms)','Value': file_data.TimeInMs }]
    return fig,data

nodetable_layout = html.Div([
    html.H2("Node Table - Assignments"),
    html.P("Select the time sweep for the assignment"),
    dcc.Slider(0, 5, 1, value=0, id='time-node-slider'),
    html.Br(),
    html.Br(),
    html.H3("Node Table"),
    dash_table.DataTable(
        id='table-editing-simple',
        columns= ([]),
        data=[],
        editable=False,
        style_table={'width':'50%'}
    ),
    html.Br(),

    dash_table.DataTable(
        id='allocator-options-node',
        columns= ([{'id': 'Allocator Options', 'name': 'Allocator Options'},{'id': 'Value', 'name': 'Value'}]),
        data=[],
        editable=False,
        style_table={'width':'200px','height':'400','margin-left':'100px','text-align':'center','flex':'1'}
    ),
])
@app.callback(
Output('table-editing-simple', 'data'),
Output('table-editing-simple','columns'),
Output('allocator-options-node','data'),
        Input('time-node-slider', 'value'))
def nodetable_layout_page(timevalue):
    #print(timevalue)
    data=load_data(timevalue)
    node_shard_list=get_data(data)
    #print(data)

    capacity_Used=[0]*len(data.ClusterState.Nodes.keys())
    QPS_Node=[0]*len(data.ClusterState.Nodes.keys())
    new_time_value=0
    prev=False
    if timevalue!=0:
        new_time_value=timevalue-1
        prev=True
    
    old_data=load_data(new_time_value)
    old_node_shard_list=get_data(old_data)

    columns= ([{'id': p, 'name': p} for p in node_params])
    if prev:
        columns.append({'id':'Previous Shard Assignment', 'name':'Previous Shard Assignment'})
    #print("Here")
    QPS_Set=False
    Cap_Set = False
    if '1' in data.ClusterState.Nodes[0].Resources:
        QPS_Set=True
    if '0' in data.ClusterState.Nodes[0].Resources:
        Cap_Set=True
    QPS_Set_Shards=False
    Cap_Set_Shards=False
    if '1' in data.ClusterState.Shards[0].Demands:
        QPS_Set_Shards=True
    if '0' in data.ClusterState.Shards[0].Demands:
        Cap_Set_Shards=True    
    for i in data.ClusterState.Nodes.keys():
        #print(i)
        for j in node_shard_list[i]:
            #print(j)
            if Cap_Set_Shards:
                capacity_Used[i]=capacity_Used[i]+data.ClusterState.Shards[j].Demands['0']
            if QPS_Set_Shards:
                QPS_Node[i]=QPS_Node[i]+data.ClusterState.Shards[j].Demands['1']
        if Cap_Set_Shards: 
            capacity_Used[i]=capacity_Used[i]/data.ClusterState.Nodes[i].Resources['0'] *100

    node_table_data=[ {
            "Node-ID":i,
            "Tags": json.dumps(data.ClusterState.Nodes[i].Tags),
            "Max Capacity": data.ClusterState.Nodes[i].Resources['0'] if Cap_Set else 'Not Set',
            "Capacity Used":capacity_Used[i]*data.ClusterState.Nodes[i].Resources['0']/100 if Cap_Set_Shards else 'N/A',
            "Max QPS": data.ClusterState.Nodes[i].Resources['1'] if QPS_Set else 'Not Set',
            "QPS on Node":QPS_Node[i] if QPS_Set_Shards else 'Not Set',
            "Assigned Shards": ','.join(map(str, sorted(node_shard_list[i]))) if node_shard_list[i] else 'No Shards Assigned'
        }
        for i in sorted(data.ClusterState.Nodes)]
    if prev:
        for i in sorted(old_data.ClusterState.Nodes):
            #new_shard_list = set(node_shard_list[i])
            #old_shard =  ','.join(map(str, sorted(old_node_shard_list[i]))) 
            node_table_data[i]["Previous Shard Assignment"]=','.join(map(str, sorted(old_node_shard_list[i]))) if old_node_shard_list[i] else 'No Shards Assigned'

    alloc_data=[{'Allocator Options':'WithCapacity','Value':"Enabled" if data.Configuration.WithCapacity else "Disabled"},
    {'Allocator Options':'WithLoadBalancing','Value':"Enabled" if data.Configuration.WithLoadBalancing else "Disabled"},
    {'Allocator Options':'WithTagAffinity','Value':"Enabled" if data.Configuration.WithTagAffinity else "Disabled"},
    {'Allocator Options':'MinimizeChurn','Value':"Enabled" if data.Configuration.WithMinimalChurn else "Disabled"},
    {'Allocator Options':'MaxChurn','Value':data.Configuration.MaxChurn},
    {'Allocator Options':'Timeout (ms)','Value':data.Configuration.SearchTimeout},
    {'Allocator Options':'Rf','Value':data.Configuration.Rf},
    {'Allocator Options':'Logging','Value':"Enabled" if data.Configuration.VerboseLogging else "Disabled"},
    {'Allocator Options': 'Time taken (ms)','Value': data.TimeInMs }]
    return node_table_data,columns,alloc_data


shardtable_layout = html.Div([
    html.H2("Shard Table - Assignments"),
    dash_table.DataTable(
        id='shard-table',
        columns= ([]),
        data=[],
        editable=False
    ),
    html.Br(),
    dcc.Slider(0, 5, 1, value=0, id='time-shard-slider'),
])
@app.callback(
Output('shard-table', 'data'),
Output('shard-table','columns'),
        Input('time-shard-slider', 'value'))
def shardtable_layout_page(timevalue):
    data=load_data(timevalue)
    #node_shard_list=get_data(data)
    QPS_Set=False
    Cap_Set=False
    if '1' in data.ClusterState.Shards[0].Demands:
        QPS_Set=True
    if '0' in data.ClusterState.Shards[0].Demands:
        Cap_Set=True
    
    columns= ([{'id': p, 'name': p} for p in shard_params])
    new_time_value=0
    prev=False
    if timevalue!=0:
        new_time_value=timevalue-1
        prev=True
    
    old_data=load_data(new_time_value)
    if prev:
        columns.append({'id':'Previous Node Assignment', 'name':'Previous Node Assignment'})
    data=[
    {
        "Shard-ID":i,
        "Tags": json.dumps(data.ClusterState.Shards[i].Tags),
        "Capacity Required": data.ClusterState.Shards[i].Demands['0'] if Cap_Set else 'Not Set',
        "QPS": data.ClusterState.Shards[i].Demands['1'] if QPS_Set else 'Not Set',
        "Assigned on Nodes": ','.join(map(str, data.ClusterState.CurrentAssignment[i]))
    }
    for i in sorted(data.ClusterState.Shards)]
    if prev:
        for i in sorted(old_data.ClusterState.Shards):
            data[i]['Previous Node Assignment']=','.join(map(str, old_data.ClusterState.CurrentAssignment[i]))

    return data,columns

        #return create_node_table(file_data,node_shard_list)

if __name__=='__main__':
    app.run_server(debug=True)
