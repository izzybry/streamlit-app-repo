# Izzy Bryant
# Last updated Dec 2022
# 03_Multi_Campaign_Comparison.py
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

def get_ra_segments(campagin_cost, app_data, user_data):
    df = pd.DataFrame(columns = ['segment', 'la', 'perc_la', 'ra', 'rac'])
    total_lvls = app_data['total_lvls'][0]
    seg = []
    ra = []
    for lvl in user_data['max_lvl']:
        perc = lvl / total_lvls
        ra.append(perc)
        if perc < .1:
            seg.append(.1)
        elif .1 <= perc < .2:
            seg.append(.2)
        elif .2 <= perc < .3:
            seg.append(.3)
        elif .3 <= perc < .4:
            seg.append(.4)
        elif .4 <= perc < .5:
            seg.append(.5)
        elif .5 <= perc < .6:
            seg.append(.6)
        elif .6 <= perc < .7:
            seg.append(.7)
        elif .7 <= perc < .8:
            seg.append(.8)
        elif .8 <= perc < .9:
            seg.append(.9)
        else:
            seg.append(1)
    user_data['seg'] = seg
    user_data['ra'] = ra
    res = user_data.groupby('seg').agg(la=('user_pseudo_id','count'), ra=('ra','mean')).reset_index()
    res['la_perc'] = res['la'] / res['la'].sum()
    res['rac'] = campaign_cost * res['la_perc'] / (res['ra'] * res['la'].sum())
    return res

# --- UI ---
st.title('Multi Campaign Comparison')
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
st.write('Total Learners Acquired During Campaign(s): ', str(len(users_df)))

daily_la = users_df.groupby(['campaign', 'LA_date'])['user_pseudo_id'].count().reset_index(name='Learners Acquired')
if len(st.session_state['campaigns']) == 1:
    daily_la['7 Day Rolling Mean'] = daily_la['Learners Acquired'].rolling(7).mean()
    daily_la['30 Day Rolling Mean'] = daily_la['Learners Acquired'].rolling(30).mean()
    daily_la_fig = px.area(daily_la,
        x='LA_date',
        y='Learners Acquired',
        color='campaign',
        labels={"LA_date": "Acquisition Date",
            'campaign': 'Campaign'},
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
    daily_la_fig = px.line(daily_la,
        x='LA_date',
        y='Learners Acquired',
        color='campaign',
        labels={'LA_date': "Acquisition Date",
            'campaign': 'Campaign'},
        title="Learners Acquired by Day")
st.plotly_chart(daily_la_fig)

ra_segs = pd.DataFrame()
for campaign in st.session_state['campaigns']:
    campaign_cost = ftm_campaigns.loc[ftm_campaigns['Campaign Name'] == campaign, 'Total Cost (USD)']
    temp = get_ra_segments(campaign_cost, ftm_apps, users_df[users_df['campaign'] == campaign])
    temp['campaign'] = campaign
    ra_segs = pd.concat([ra_segs, temp])

ra_segs_fig = px.bar(ra_segs,
    x='seg',
    y='la_perc',
    color='campaign',
    barmode='group',
    hover_data=['rac', 'la'],
    labels={
        'la_perc': '% LA',
        'seg': 'RA Decile',
        'rac': 'RAC (USD)',
        'la': 'LA'
    },
    title='LA by RA Decile' 
)
st.plotly_chart(ra_segs_fig)

# monthly_la = users_df
# monthly_la['LA_YM'] = (pd.to_datetime(monthly_la.LA_date)).dt.strftime('%Y-%m')
# monthly_la = users_df.groupby(['campaign', 'LA_YM'])['user_pseudo_id'].count().reset_index(name='Learners Acquired')
# monthly_la_fig = px.bar(monthly_la,
#     x='LA_YM',
#     y='Learners Acquired',
#     color='campaign',
#     labels={'LA_YM': 'Acquisition Date (Month)',
#         'campaign': 'Campaign'},
#     title="Learners Acquired by Month"
# )
# st.plotly_chart(monthly_la_fig)

# country_la = users_df.groupby(['country'])['user_pseudo_id'].count().reset_index(name='Learners Acquired')
# country_fig = px.choropleth(country_la,
#     locations='country',
#     color='Learners Acquired',
#     color_continuous_scale='Emrld',
#     locationmode='country names',
#     title='Learners Acquired by Country')
# country_fig.update_layout(geo=dict(bgcolor= 'rgba(0,0,0,0)'))
# st.plotly_chart(country_fig)
