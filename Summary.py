# -*- coding: utf-8 -*-
# Izzy Bryant
# Last updated Dec 2022
# Summary.py
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

st.set_page_config(page_title="Summary")

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

# @st.cache
def get_campaign_data():
    campaign_sheet_url = st.secrets["Campaign_gsheets_url"]
    campaign_rows = run_query(f'SELECT * FROM "{campaign_sheet_url}"')
    campaign_data = pd.DataFrame(columns = ['Campaign Name', 'Language', 'Country', 'Start Date', 'End Date', 'Total Cost (USD)'],
                            data = campaign_rows)
    campaign_data['Start Date'] = (pd.to_datetime(campaign_data['Start Date'])).dt.date
    campaign_data['End Date'] = (pd.to_datetime(campaign_data['End Date'])
                                    + pd.DateOffset(months=1) - pd.Timedelta(1, unit='D')).dt.date
    campaign_data = campaign_data.astype({
        'Total Cost (USD)': 'float'
    })
    return campaign_data

# @st.cache
def get_user_data():
    sql_query = f"""
        SELECT * FROM `dataexploration-193817.user_data.ftm_users`
    """
    rows_raw = client.query(sql_query)
    rows = [dict(row) for row in rows_raw]
    df = pd.DataFrame(rows)
    df['LA_date'] = (pd.to_datetime(df['LA_date'])).dt.date
    df['max_lvl_date'] = (pd.to_datetime(df['max_lvl_date'])).dt.date
    return df

# @st.cache
def get_apps_data():
    apps_sheet_url = st.secrets["ftm_apps_gsheets_url"]
    apps_rows = run_query(f'SELECT app_id, language, bq_property_id, bq_project_id, total_lvls FROM "{apps_sheet_url}"')
    apps_data = pd.DataFrame(columns = ['app_id', 'language', 'bq_property_id', 'bq_project_id', 'total_lvls'],
        data = apps_rows)
    return apps_data

# @st.cache
def get_campaign_metrics():
    camp_metrics_url = st.secrets["campaign_metrics_gsheets_url"]
    camp_metrics_rows = run_query(f'SELECT * FROM "{camp_metrics_url}"')
    camp_metrics_data = pd.DataFrame(columns = ['campaign_name', 'la', 'lac', 'ra', 'rac'],
                            data = camp_metrics_rows)
    return camp_metrics_data


# --- UI ---
st.title('CL FTM Metrics Dashboard: Summary')
expander = st.expander('Definitions')
expander.write('Learner Acquisition (LA) = number of users that have successfully completed at least one FTM level')
expander.write('Learner Acquisition Cost (LAC) = the cost (USD) of acquiring one learner')
expander.write('Reading Acquisition (RA) = the average percentage of FTM levels completed per learner')
expander.write('Reading Acquisition Cost (RAC) = the cost (USD) of acquiring the average amount of reading per learner')

ftm_campaigns = get_campaign_data()
select_campaigns = st.sidebar.multiselect(
    "Select Campaign(s)",
    ftm_campaigns['Campaign Name'],
    ftm_campaigns['Campaign Name'],
    key = 'campaigns'
)

# LEARNER & READING ACQUISITION COST
ftm_campaign_metrics = get_campaign_metrics()
ftm_campaign_metrics = ftm_campaign_metrics[ftm_campaign_metrics['campaign_name'].isin(st.session_state['campaigns'])]
ftm_campaign_metrics['camp_age'] = [(ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == c, 'End Date'].item() - ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == c, 'Start Date'].item()).days for c in ftm_campaign_metrics['campaign_name']]
lavslac = px.scatter(ftm_campaign_metrics,
    x='la',
    y='lac',
    color='campaign_name',
    size='camp_age',
    labels={
        'la': 'Learners Acquired',
        'lac': 'Learner Acquisition Cost',
        'camp_age': 'Campaign Age (Days)',
        'campaign_name': 'Campaign Name'
    },
    title='LA vs LAC'    
)
st.plotly_chart(lavslac)

ravsrac = px.scatter(ftm_campaign_metrics,
    x='ra',
    y='rac',
    color='campaign_name',
    size='camp_age',
    labels={
        'ra': 'Reading Acquired',
        'rac': 'Reading Acquisition Cost',
        'camp_age': 'Campaign Age (Days)',
        'campaign_name': 'Campaign Name'
    },
    title='RA vs RAC'    
)
st.plotly_chart(ravsrac)
