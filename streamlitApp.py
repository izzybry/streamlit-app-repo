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

container = st.sidebar.container()
select_all = st.sidebar.checkbox("Select All App Buckets", value=True)
 
if select_all:
    select_bucket = container.multiselect(
        "Select App Bucket(s)",
        ["FTM - English", "FTM - Afrikaans", "FTM - AppBucket1", "FTM-AppBucket2",
            "FTM-AppBucket3", "FTM - French", "FTM - isiXhosa", "FTM - Kinayrwanda",
            "FTM - Oromo", "FTM - SePedi", "FTM - Somali", "FTM - SouthAfricanEnglish",
            "FTM - Spanish", "FTM - Swahili", "FTM - Zulu"],
        ["FTM - English", "FTM - Afrikaans", "FTM - AppBucket1", "FTM-AppBucket2",
            "FTM-AppBucket3", "FTM - French", "FTM - isiXhosa", "FTM - Kinayrwanda",
            "FTM - Oromo", "FTM - SePedi", "FTM - Somali", "FTM - SouthAfricanEnglish",
            "FTM - Spanish", "FTM - Swahili", "FTM - Zulu"],
        key = 'buckets'
    )
else:
    select_bucket = container.multiselect(
        "Select App Bucket(s)",
        ["FTM - English", "FTM - Afrikaans", "FTM - AppBucket1", "FTM-AppBucket2",
            "FTM-AppBucket3", "FTM - French", "FTM - isiXhosa", "FTM - Kinayrwanda",
            "FTM - Oromo", "FTM - SePedi", "FTM - Somali", "FTM - SouthAfricanEnglish",
            "FTM - Spanish", "FTM - Swahili", "FTM - Zulu"],
        key = 'buckets'
    )

countries_df = pd.read_csv('countries.csv')

container2 = st.sidebar.container()
select_all2 = st.sidebar.checkbox("Select All Countries", value=True)

if select_all2:
    select_country = container2.multiselect(
        "Filter Countries",
        countries_df['name'],
        countries_df['name'],
        key = 'country_names'
    )
else:
    select_country = container2.multiselect(
    "Filter Countries",
    countries_df['name'],
    key = 'country_names'
)

app_propertyID_dict = {
    "FTM - English": "152408808",
    "FTM - Afrikaans": "177200876",
    "FTM - AppBucket1": "174638281",
    "FTM-AppBucket2": "161789655",
    "FTM-AppBucket3": "159643920",
    "FTM - French": "173880465",
    "FTM - isiXhosa": "180747962",
    "FTM - Kinayrwanda": "177922191",
    "FTM - Oromo": "167539175",
    "FTM - SePedi": "180755978",
    "FTM - Somali": "159630038",
    "FTM - SouthAfricanEnglish": "173750850",
    "FTM - Spanish": "158656398",
    "FTM - Swahili": "160694316",
    "FTM - Zulu": "155849122"
}

# get Google Analytics credentials from secrets
ga_credentials = service_account.Credentials.from_service_account_info(
    st.secrets["GOOGLE_APPLICATION_CREDENTIALS"]
)
# set global report variables
dimension_list = ['date', 'country', 'countryId']
metric_list = ['active1DayUsers', 'active7DayUsers', 'active28DayUsers',
                'newUsers', 'dauPerMau', 'dauPerWau', 'wauPerMau']
date_range = (st.session_state['date_range'][0].strftime('%Y-%m-%d'),
                st.session_state['date_range'][1].strftime('%Y-%m-%d'))

# get data for all selected language / app buckets
main_df = pd.DataFrame()
for i in st.session_state['buckets']:
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

# Filter main_df
country_ids_list = countries_df[countries_df.name.isin(st.session_state['country_names'])]['alpha2']
country_ids_list = [x.upper() for x in country_ids_list]
main_df = main_df[main_df.countryId.isin(country_ids_list)]


# Convert date column to datetime, then sort by date
main_df['date'] = pd.to_datetime(main_df['date'])
main_df['date'] = main_df['date'].dt.date
main_df = main_df.sort_values(by=['date'])

# Convert user count columns to ints
main_df = main_df.astype({
    'active1DayUsers': 'int32',
    'active7DayUsers': 'int32',
    'active28DayUsers': 'int32',
    'newUsers': 'int32',
    'dauPerMau': 'float',
    'dauPerWau': 'float',
    'wauPerMau': 'float'
})

# --- Active Users by Country ---
users_by_country_df = main_df[
    ['country', 'active1DayUsers']
]

users_by_country_df = users_by_country_df.groupby(['country']).sum().reset_index()

country_fig = px.choropleth(users_by_country_df, locations='country',
                                    color='active1DayUsers',
                                    color_continuous_scale='Emrld',
                                    locationmode='country names',
                                    title='Active Users by Country',
                                    labels={'active1DayUsers':'Active Users'})
country_fig.update_layout(geo=dict(bgcolor= 'rgba(0,0,0,0)'))
st.plotly_chart(country_fig)

# --- New Users by Country ---
new_users_by_country_df = main_df[
    ['country', 'newUsers']
]

new_users_by_country_df = new_users_by_country_df.groupby(['country']).sum().reset_index()

country_fig2 = px.choropleth(new_users_by_country_df, locations='country',
                                    color='newUsers',
                                    color_continuous_scale='Emrld',
                                    locationmode='country names',
                                    title='New Users by Country',
                                    labels={'newUsers':'New Users'})
country_fig2.update_layout(geo=dict(bgcolor= 'rgba(0,0,0,0)'))
st.plotly_chart(country_fig2)

# --- Users Stickiness ---
user_stickiness_df = main_df[
    ['date', 'dauPerMau', 'dauPerWau', 'wauPerMau']
]
user_stickiness_df['dauPerMau'] = round(user_stickiness_df['dauPerMau'], 1)
user_stickiness_df['dauPerWau'] = round(user_stickiness_df['dauPerWau'], 1)
user_stickiness_df['wauPerMau'] = round(user_stickiness_df['wauPerMau'], 1)

user_stickiness_df = user_stickiness_df.groupby(['date']).sum().reset_index()

user_stickiness_fig = px.line(user_stickiness_df, x='date',
                            y=['dauPerMau', 'dauPerWau', 'wauPerMau'],
                                title = "User Stickiness",
                                labels={'value': '%', 'date': 'Date'})
st.plotly_chart(user_stickiness_fig)

# --- Active Users by Day ---
active_users_df = main_df[
    ['date', 'active1DayUsers', 'active7DayUsers', 'active28DayUsers']
]

# Group data by day and plot as timeseries chart
active_users_df = active_users_df.groupby(['date']).sum().reset_index()

active_users_fig = px.line(active_users_df, x='date',
                            y=['active1DayUsers', 'active7DayUsers', 'active28DayUsers'],
                                title = "Active Users by Day",
                                labels={"value": "Active Users",
                                    "date": "Date", "variable": "Trailing"})
st.plotly_chart(active_users_fig)


# --- Raw Data ---
st.write(main_df)
  # -----------------------------------------
