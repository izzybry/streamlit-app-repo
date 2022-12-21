# -*- coding: utf-8 -*-
# Izzy Bryant
# Last updated Dec 2022
# streamlitApp.py
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

# --- GLOBAL VARIABLES ---
countries_df = pd.read_csv('countries.csv')

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

def get_apps_data():
    apps_sheet_url = st.secrets["ftm_apps_gsheets_url"]
    apps_rows = run_query(f'SELECT app_id, language, bq_property_id, bq_project_id, total_lvls FROM "{apps_sheet_url}"')
    apps_data = pd.DataFrame(columns = ['app_id', 'language', 'bq_property_id', 'bq_project_id', 'total_lvls'],
        data = apps_rows)
    return apps_data

def get_campaign_metrics():
    camp_metrics_url = st.secrets["campaign_metrics_gsheets_url"]
    camp_metrics_rows = run_query(f'SELECT * FROM "{camp_metrics_url}"')
    camp_metrics_data = pd.DataFrame(columns = ['campaign_name', 'la', 'lac', 'ra', 'rac'],
                            data = camp_metrics_rows)
    return camp_metrics_data

# --- UI ---
st.title('Curious Learning')
expander = st.expander('Definitions')
expander.write('Learner Acquisition = number of users that have successfully completed at least one FTM level')
expander.write('Learner Acquisition Cost = the cost (USD) of acquiring one learner')
expander.write('Reading Acquisition = the average percentage of FTM levels completed across learner cohort')
expander.write('Reading Acquisition Cost = the cost (USD) of acquiring the average amount of reading across learner cohort')

ftm_campaigns = get_campaign_data()
select_campaigns = st.sidebar.multiselect(
    "Select Campaign(s)",
    ftm_campaigns['Campaign Name'],
    ftm_campaigns['Campaign Name'][0],
    key = 'campaigns'
)

# DAILY LEARNERS ACQUIRED
ftm_users = get_user_data()
ftm_apps = get_apps_data()
users_df = pd.DataFrame()
for campaign in st.session_state['campaigns']:
    start_date = ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == campaign, 'Start Date'].item()
    end_date = ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == campaign, 'End Date'].item()
    language = ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == campaign, 'Language'].item()
    app = ftm_apps.loc[ftm_apps['language'] == language, 'app_id'].item()
    country = ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == campaign, 'Country'].item()
    if country == 'All':
        temp = ftm_users[(ftm_users['LA_date'] >= start_date) & (ftm_users['LA_date'] <= end_date) & (ftm_users['app_id'] == app)]
    else:
        temp = ftm_users[(ftm_users['LA_date'] >= start_date) & (ftm_users['LA_date'] <= end_date) & (ftm_users['app_id'] == app) & (ftm_users['country'] == country)]
    temp['campaign'] = campaign
    users_df = pd.concat([users_df, temp])

daily_la = users_df.groupby(['campaign', 'LA_date'])['user_pseudo_id'].count().reset_index(name='Learners Acquired')
if len(st.session_state['campaigns']) == 1:
    daily_la['7 Day Rolling Mean'] = daily_la['Learners Acquired'].rolling(7).mean()
    daily_la['30 Day Rolling Mean'] = daily_la['Learners Acquired'].rolling(30).mean()
    daily_la_fig = px.area(daily_la,
        x='LA_date',
        y='Learners Acquired',
        color='campaign',
        labels={"LA_date": "Acquisition Date"},
        title="Learners Acquired by Day")
    rm_fig = px.line(daily_la,
        x='LA_date',
        y=['7 Day Rolling Mean', '30 Day Rolling Mean'],
        color_discrete_map={
            '7 Day Rolling Mean': 'green',
            '30 Day Rolling Mean': 'red'
        })
    daily_la_fig.add_trace(rm_fig.data[0])
    daily_la_fig.add_trace(rm_fig.data[1])
else:
    daily_la_fig = px.area(daily_la,
        x='LA_date',
        y='Learners Acquired',
        color='campaign',
        labels={"LA_date": "Acquisition Date"},
        title="Learners Acquired by Day")
st.plotly_chart(daily_la_fig)

country_la = users_df.groupby(['country'])['user_pseudo_id'].count().reset_index(name='Learners Acquired')
country_fig = px.choropleth(country_la,
    locations='country',
    color='Learners Acquired',
    color_continuous_scale='Emrld',
    locationmode='country names',
    title='Learners Acquired by Country')
country_fig.update_layout(geo=dict(bgcolor= 'rgba(0,0,0,0)'))
st.plotly_chart(country_fig)

# LEARNER ACQUISITION COST
ftm_campaign_metrics = get_campaign_metrics()
ftm_campaign_metrics = ftm_campaign_metrics[ftm_campaign_metrics['campaign_name'].isin(st.session_state['campaigns'])]
ftm_campaign_metrics['camp_age'] = [(pd.to_datetime("today").date() - ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == c, 'Start Date'].item()).days for c in ftm_campaign_metrics['campaign_name']]
st.write('ftm_campaign_metrics', ftm_campaign_metrics)  
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

ftm_campaign_metrics['avg_ra_per_learner'] = ftm_campaign_metrics['ra'] / ftm_campaign_metrics['la']
avgravsrac = px.scatter(ftm_campaign_metrics,
    x='avg_ra_per_learner',
    y='rac',
    color='campaign_name',
    size='camp_age',
    labels={
        'avg_ra_per_learner': 'Avg Reading Acquired / Learner',
        'rac': 'Reading Acquisition Cost',
        'camp_age': 'Campaign Age (Days)',
        'campaign_name': 'Campaign Name'
    },
    title='Average RA / Learner vs RAC'
)
st.plotly_chart(avgravsrac)
