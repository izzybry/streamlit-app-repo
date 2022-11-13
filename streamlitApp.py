# -*- coding: utf-8 -*-
# Izzy Bryant
# Last updated Nov 2022
# streamlitApp.py
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

# https://learndataanalysis.org/source-code-automate-google-analytics-4-ga4-reporting-with-python-step-by-step-tutorial/
# from jj_data_connector.ga4 import GA4Report, Metrics, Dimensions
import ga4
from google.analytics.data_v1beta import BetaAnalyticsDataClient
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
select_date_range = st.sidebar.date_input(
    "Select Date Range:",
    ((pd.to_datetime("today").date() - pd.Timedelta(28, unit='D')),
        pd.to_datetime("today").date()),
    key = 'date_range'
)

select_language = st.sidebar.multiselect(
    "Select Language / App",
    ["FTM - English", "FTM - Afrikaans"],
    ["FTM - English", "FTM - Afrikaans"],
    key = 'language_app'
)

app_propertyID_dict = {
    "FTM - English": "152408808",
    "FTM - Afrikaans": "177200876"
}

# get Google Analytics credentials from secrets
ga_credentials = service_account.Credentials.from_service_account_info(
    st.secrets["GOOGLE_APPLICATION_CREDENTIALS"]
)
# set global report variables
dimension_list = ['date', 'country']
metric_list = ['active1DayUsers', 'active7DayUsers', 'active28DayUsers',
                'newUsers']
date_range = (st.session_state['date_range'][0].strftime('%Y-%m-%d'),
                st.session_state['date_range'][1].strftime('%Y-%m-%d'))

# get data for all selected language / app buckets
main_df = pd.DataFrame()
for i in st.session_state['language_app']:
    property_id = app_propertyID_dict[i]
    ga4_report = ga4.GA4Report(property_id, ga_credentials)
    report_df = ga4_report.run_report(
        dimension_list, metric_list,
        date_ranges = [date_range],
        #row_limit = 1000,
        offset_row = 0
    )
    report_df = pd.DataFrame(columns = report_df['headers'],
                                    data = report_df['rows'])
    report_df.insert(1, "Language/App Bucket", i)
    main_df = pd.concat([main_df, report_df])

# Convert date column to datetime, then sort by date
main_df['date'] = pd.to_datetime(main_df['date'])
main_df['date'] = main_df['date'].dt.date
main_df = main_df.sort_values(by=['date'])
st.write(main_df)

# --- Active Users by Day ---
active_users_df = main_df[
    ['date', 'active1DayUsers', 'active7DayUsers', 'active28DayUsers']]

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
