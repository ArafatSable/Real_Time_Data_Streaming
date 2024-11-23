from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from pymongo import MongoClient
import dash_bootstrap_components as dbc

# Initialize Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# MongoDB connection (adjust connection parameters as necessary)
client = MongoClient("mongodb://localhost:27017/")
db = client['ecommerce']

# Function to fetch data from MongoDB and convert it to Pandas DataFrame
def fetch_data(collection_name):
    collection = db[collection_name]
    data = list(collection.find())
    return pd.DataFrame(data)

# Layout of the Dash app
app.layout = dbc.Container([
    html.H1("Real-Time Ecommerce Data Dashboard", className="text-center my-3"),
    
    # Dropdown to select collection type
    dbc.Row([
        dbc.Col(dcc.Dropdown(
            id='data-type',
            options=[
                {'label': 'Customer Segments', 'value': 'customer_segments'},
                {'label': 'City Segments', 'value': 'city_segments'},
                {'label': 'At-Risk Customers', 'value': 'at_risk_customers'}
            ],
            value='customer_segments',
            clearable=False,
        ), width=4)
    ], justify='center'),
    
    # Graph to show real-time data
    dbc.Row([
        dbc.Col(dcc.Graph(id="live-graph"), width=12)
    ]),
    
    # Interval component for auto-refresh
    dcc.Interval(
        id='interval-component',
        interval=10*1000,  # Refresh every 10 seconds
        n_intervals=0
    )
], fluid=True)

# Callback to update graph based on selected data type
@app.callback(
    Output('live-graph', 'figure'),
    [Input('data-type', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_graph(data_type, n_intervals):
    # Fetch data from MongoDB
    df = fetch_data(data_type)

    # Generate plots based on collection type
    if data_type == 'customer_segments':
        fig = px.bar(df, x="membership_type", y="avg_total_spend", color="age_group",
                     title="Average Total Spend by Membership Type and Age Group",
                     labels={'avg_total_spend': 'Average Spend', 'membership_type': 'Membership Type'})
    elif data_type == 'city_segments':
        fig = px.bar(df, x="city", y="total_items_purchased", color="city",
                     title="Total Items Purchased by City",
                     labels={'total_items_purchased': 'Total Items Purchased', 'city': 'City'})
    elif data_type == 'at_risk_customers':
        fig = px.scatter(df, x="days_since_last_purchase", y="total_spend", color="city",
                         size="days_since_last_purchase", hover_data=["customer_id"],
                         title="At-Risk Customers: Spend vs. Days Since Last Purchase",
                         labels={'total_spend': 'Total Spend', 'days_since_last_purchase': 'Days Since Last Purchase'})
    else:
        fig = {}

    fig.update_layout(transition_duration=500)
    return fig

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)
