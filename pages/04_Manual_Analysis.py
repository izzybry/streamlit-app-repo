# Izzy Bryant
# Last updated Dec 2022
# 04_Manual_Analysis.py
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from gsheetsdb import connect
import datetime
import pandas as pd
import db_dtypes
import json
import plotly
import plotly.express as px
import plotly.graph_objects as go

# --- DATA ---
# Create a Google Sheets connection object.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
    ]
)
conn = connect(credentials=credentials)
# Create BigQuery API client.
bq_credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=bq_credentials)

def run_query(query):
    rows = conn.execute(query, headers=1)
    rows = rows.fetchall()
    return rows

@st.experimental_memo
def get_apps_data():
    apps_sheet_url = st.secrets["ftm_apps_gsheets_url"]
    apps_rows = run_query(f'SELECT app_id, language, bq_property_id, bq_project_id, total_lvls FROM "{apps_sheet_url}"')
    apps_data = pd.DataFrame(columns = ['app_id', 'language', 'bq_property_id', 'bq_project_id', 'total_lvls'],
        data = apps_rows)
    return apps_data

# --- UI ---
st.title('Manual Analysis')
expander = st.expander('Definitions')
expander.write('Learner Acquisition (LA) = number of users that have successfully completed at least one FTM level')
expander.write('Learner Acquisition Cost (LAC) = the cost (USD) of acquiring one learner')
expander.write('Reading Acquisition (RA) = the average percentage of FTM levels completed per learner')
expander.write('Reading Acquisition Cost (RAC) = the cost (USD) of acquiring the average amount of reading per learner')
select_date_range = st.sidebar.date_input(
    'Select date range',
    (pd.to_datetime("today").date() - pd.Timedelta(30, unit='d'),
        pd.to_datetime("today").date() - pd.Timedelta(1, unit='d')),
    key='date_range'
)
ftm_apps = get_apps_data()
langs = ftm_apps['language']
select_languages = st.sidebar.multiselect(
    'Select languages',
    langs,
    default=langs,
    key='languages'
)
countries_df = pd.read_csv('countries.csv')
select_countries = st.sidebar.multiselect(
    'Select countries',
    countries_df['name'],
    default=countries_df['name'],
    key='countries'
)