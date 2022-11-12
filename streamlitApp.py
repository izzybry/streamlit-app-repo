# -*- coding: utf-8 -*-
# Izzy Bryant
# Last updated Nov 2022
# streamlitApp.py
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

# https://learndataanalysis.org/source-code-automate-google-analytics-4-ga4-reporting-with-python-step-by-step-tutorial/
from jj_data_connector.ga4 import GA4Report, Metrics, Dimensions
import os

import datetime
import pandas as pd
import db_dtypes
import json

import plotly
import plotly.express as px
from plotly.graph_objs import *

# BIGQUERY SET UP
# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Title and subtitle
st.title('Curious Learning')
st.header('User Acquisition and Engagement Metrics')

# Set up sidebar with date picker
add_date_range = st.sidebar.date_input(
    "Select Date Range:",
    ((pd.to_datetime("today").date() - pd.Timedelta(28, unit='D')),
        pd.to_datetime("today").date()),
    key = 'date_range'
)

# Set sql query, parameters, and job config
# sql_query = """
#     SELECT event_date, event_name, properties, traffic_source.source, traffic_source.name, geo.country, geo.city
#     FROM `ftm-hindi.analytics_174638281.events_20*`,
#     UNNEST (user_properties) as properties
#     WHERE parse_date('%y%m%d', _table_suffix) between @start and @end
#     and event_name = 'first_open'
# """
sql_query = """
    SELECT event_date, event_name, properties, traffic_source.source, traffic_source.name, geo.country, geo.city
    FROM `ftm-english.analytics_152408808.events_20*`,
    UNNEST (user_properties) as properties
    WHERE parse_date('%y%m%d', _table_suffix) between @start and @end
    and event_name = 'first_open'
"""

query_parameters = [
    bigquery.ScalarQueryParameter("start", "DATE", st.session_state['date_range'][0]),
    bigquery.ScalarQueryParameter("end", "DATE", st.session_state['date_range'][1])
]
job_config = bigquery.QueryJobConfig(
    query_parameters = query_parameters
)

# Reference: https://docs.streamlit.io/knowledge-base/tutorials/databases/bigquery
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
#@st.experimental_memo(ttl=600)
def run_query(query, _config):
    rows_raw = client.query(query, job_config = _config)
    # Convert to list of dicts. Required for st.experimental_memo to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows
    
rows = run_query(sql_query, job_config)

# st.write(sql_query)

df = pd.DataFrame(rows)
df = df.sort_values(by=['event_date'])

# st.write("raw df len = " + str(df.shape[0]))
# st.write(df)

# Convert event_date column to datetime
df['event_date'] = pd.to_datetime(df['event_date'])
df['event_date'] = df['event_date'].dt.date

st.write("df len = " + str(df.shape[0]))
st.write(df)

# user acquisition metrics
st.subheader('User Acquisition')

usersByDay = df[df['event_name'] == 'first_open'].groupby(
    ['event_date'])['event_date'].count().reset_index(name='count')

#usersByDay

usersByDayFig = px.line(usersByDay, x='event_date', y='count', labels={
                     "event_date": "Date (Day)",
                     "count": "New Users"},
                     title = "New User Count by Day")
st.plotly_chart(usersByDayFig)

usersByWeek = usersByDay
usersByWeek['event_date'] = pd.to_datetime(usersByWeek['event_date']) - pd.to_timedelta(6, unit='d')
usersByWeek = usersByDay.groupby(
    [pd.Grouper(key='event_date', freq='W')])['count'].sum().reset_index()

#st.write(usersByWeek)

usersByWeekFig = px.line(usersByWeek, x='event_date', y='count', labels={
                     'event_date': "Week Starting Date",
                     'count': "New Users"},
                     title = "New User Count by Week")
st.plotly_chart(usersByWeekFig)

df_source_count = df.groupby(["source"])["source"].count().reset_index(name="count")
fig = px.pie(df_source_count, values='count', names='source')
fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)'})
st.plotly_chart(fig)

df_country_count = df.groupby(["country"])["country"].count().reset_index(name="count")

st.write("df_country_count len = " + str({df_country_count.shape[0]}))
st.write(df_country_count)

# , locationmode=‘country names’)
fig = px.choropleth(df_country_count, locations='country', color='count',
                    locationmode="country names")
fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)'})
st.plotly_chart(fig)

# df = pd.concat([df_raw, df_traffic_source], axis=1)
# st.write(f"df len = {df.size}")
# st.write(df)


# for r in df.iterrows():
#     st.write(r)

st.markdown("""---""")
st.subheader('Google Analytics Data')

# GOOGLE ANALYTICS DATA API SET UP
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'GA4_service_accounts/ftm-afrikaans_service_account_key.json'
property_id = '177200876'

# creating GA4Report object instance
ga4 = GA4Report(property_id)

# variables
dimension_list = ['date', 'country']
metric_list = ['active1DayUsers', 'active7DayUsers', 'active28DayUsers',
                'newUsers']
date_range = (st.session_state['date_range'][0].strftime('%Y-%m-%d'),
                st.session_state['date_range'][1].strftime('%Y-%m-%d'))

# run the GA4 report
report_df = ga4.run_report(
    dimension_list, metric_list,
    date_ranges = [date_range],
    #row_limit = 1000,
    offset_row = 0
)

report_df = pd.DataFrame(columns = report_df['headers'], data = report_df['rows'])

# Convert date column to datetime, then sort by date
report_df['date'] = pd.to_datetime(report_df['date'])
report_df['date'] = report_df['date'].dt.date
report_df = report_df.sort_values(by=['date'])

# --- Active Users by Day ---
active_users_df = report_df[['date', 'active1DayUsers', 'active7DayUsers', 'active28DayUsers']]

active_users_df = active_users_df.astype({
    'active1DayUsers': 'int32',
    'active7DayUsers': 'int32',
    'active28DayUsers': 'int32'})

# Group data by day and plot as timeseries chart
active_users_df = active_users_df.groupby(['date']).sum().reset_index()
st.write(active_users_df)

active_users_fig = px.line(active_users_df, x='date',
                            y=['active1DayUsers', 'active7DayUsers', 'active28DayUsers'],
                                title = "Active Users by Day",
                                labels={"value": "Active Users", "date": "Date", "variable": "Trailing"})
st.plotly_chart(active_users_fig)
  # -----------------------------------------
