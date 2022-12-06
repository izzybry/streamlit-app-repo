# -*- coding: utf-8 -*-
# Izzy Bryant
# Last updated Nov 2022
# streamlitApp.py
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from gsheetsdb import connect

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
import plotly.graph_objects as go
from plotly.graph_objs import *

# Globalal variables
app_propertyID_dict = {
    "FTM - English": "152408808",
    "FTM - Afrikaans": "177200876",
    "FTM - AppBucket1": "174638281",
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

app_bqID_dict = {
    "FTM - English": "ftm-english",
    "FTM - Afrikaans": "ftm-afrikaans",
    "FTM - AppBucket1": "ftm-hindi",
    "FTM-AppBucket2": "ftm-brazilian-portuguese",
    "FTM-AppBucket3": "ftm-b9d99",
    "FTM - French": "ftm-french",
    "FTM - isiXhosa": "ftm-isixhosa",
    "FTM - Kinayrwanda": "ftm-kinayrwanda",
    "FTM - Oromo": "ftm-oromo",
    "FTM - SePedi": "ftm-sepedi",
    "FTM - Somali": "ftm-somali",
    "FTM - SouthAfricanEnglish": "ftm-southafricanenglish",
    "FTM - Spanish": "ftm-spanish",
    "FTM - Swahili": "ftm-swahili",
    "FTM - Zulu": "ftm-zulu"
}

# # Need to confirm mappings below
# app_info_id_lang_dict = {
#     "com.eduapp4syria.feedthemonsterENGLISH": "English", 
#     "com.feedthemonsterENGLISHonto.kgl": "English",
#     "com.eduapp4syria.feedthemonsterMarathi": "Marathi",
#     "com.eduapp4syria.feedthemonsterNepali": "Nepali",
#     "com.eduapp4syria.feedthemonsterKurdish": "Kurdish",
#     "com.eduapp4syria.feedthemonsterArabic": "Arabic",
#     "com.eduapp4syria.feedthemonsterHindi": "Hindi",
#     "com.eduapp4syria.feedthemonsterPashto": "Pashto",
#     "com.eduapp4syria.feedthemonsterTajik": "Tajik",
#     "com.eduapp4syria.feedthemonsterAmharic": "Amharic",
#     "com.eduapp4syria.feedthemonsterWolof": "Wolof",
#     "com.eduapp4syria.feedthemonsterMalay": "Malay",
#     "com.eduapp4syria.feedthemonsterThai": "Thai",
#     "com.eduapp4syria.feedthemonsterAfrikaans": "Afrikaans",
#     "com.eduapp4syria.feedthemonsterBangla": "Bangala",
#     "com.eduapp4syria.feedthemonsterLugandan": "Ganda",
#     "com.eduapp4syria.feedthemonsterSiswati": "Siswati",
#     "com.eduapp4syria.feedthemonsterTsonga": "Tsonga",
#     "com.eduapp4syria.feedthemonsterWAENGLISH": "unknown", #??
#     "com.eduapp4syria.feedthemonsterEnglishIndian": "unknown", #??
#     "com.eduapp4syria.feedthemonsterENGLISHAUS": "unknown", #??
#     "com.eduapp4syria.feedthemonsterBrazilianPortuguese": "Portuguese",
#     "com.eduapp4syria.feedthemonsterMalagasy": "Malagasy",
#     "com.eduapp4syria.feedthemonsterTagalog": "Tagalog",
#     "com.eduapp4syria.feedthemonsterTswana": "Tswana",
#     "com.eduapp4syria.feedthemonsterSesotho": "Sesotho",
#     "com.eduapp4syria.feedthemonsterVenda": "Venda",
#     "com.eduapp4syria.feedthemonsterHatianCreole": "Haitian; Haitian Creole",
#     "com.eduapp4syria.feedthemonsterHausa": "Hausa",
#     "com.eduapp4syria.feedthemonsterFarsi": "Farsi",
#     "com.eduapp4syria.feedthemonsterGeorgian": "Georgian",
#     "com.eduapp4syria.feedthemonsterUkranian": "Ukranian",
#     "com.eduapp4syria.feedthemonsterTURKISH": "Turkish",
#     "com.eduapp4syria.feedthemonsterFrench": "French",
#     "com.eduapp4syria.feedthemonsterisiXhosa": "Xhosa",
#     "com.eduapp4syria.feedthemonsterKinyarwanda": "Kinyarwanda",
#     "com.eduapp4syria.feedthemonsterOromo": "Oromo",
#     "com.eduapp4syria.feedthemonsterSePedi": "SePedi",
#     "com.eduapp4syria.feedthemonsterSOMALI": "Somali",
#     "com.eduapp4syria.feedthemonsterSAEnglish": "English", #?? south african english, should we differentiate?
#     "com.eduapp4syria.feedthemonsterSPANISH": "Spanish",
#     "com.eduapp4syria.feedthemonsterSwahili": "Swahili",
#     "com.eduapp4syria.FeedTheMonsterZULU": "Zulu",
#     "com.eduapp4syria.feedthemonsterAzerbaijani": "Azerbaijani",
#     "com.eduapp4syria.feedthemonsterVietnamese": "Vietnamese",
#     "com.eduapp4syria.feedthemonsterJavanese": "Javanese",
#     "com.eduapp4syria.feedthemonsterIgbo": "Igbo",
#     "com.eduapp4syria.feedthemonsterShona": "Shona",
#     "com.eduapp4syria.feedthemonsterYoruba": "Yoruba"
# }

appLang_bucket_dict = {
    "com.eduapp4syria.feedthemonsterENGLISH": "FTM - English", 
    "com.feedthemonsterENGLISHonto.kgl": "FTM - English",
    "com.eduapp4syria.feedthemonsterMarathi": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterNepali": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterKurdish": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterArabic": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterHindi": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterPashto": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterTajik": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterAmharic": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterWolof": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterMalay": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterThai": "FTM - AppBucket1",
    "com.eduapp4syria.feedthemonsterAfrikaans": "FTM - Afrikaans",
    "com.eduapp4syria.feedthemonsterBangla": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterLugandan": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterSiswati": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterTsonga": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterWAENGLISH": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterEnglishIndian": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterENGLISHAUS": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterBrazilianPortuguese": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterMalagasy": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterTagalog": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterTswana": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterSesotho": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterVenda": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterHatianCreole": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterHausa": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterFarsi": "FTM-AppBucket3",
    "com.eduapp4syria.feedthemonsterGeorgian": "FTM-AppBucket3",
    "com.eduapp4syria.feedthemonsterUkranian": "FTM-AppBucket3",
    "com.eduapp4syria.feedthemonsterTURKISH": "FTM-AppBucket3",
    "com.eduapp4syria.feedthemonsterFrench": "FTM - French",
    "com.eduapp4syria.feedthemonsterisiXhosa": "FTM - isiXhosa",
    "com.eduapp4syria.feedthemonsterKinyarwanda": "FTM - Kinayrwanda",
    "com.eduapp4syria.feedthemonsterOromo": "FTM - Oromo",
    "com.eduapp4syria.feedthemonsterSePedi": "FTM - SePedi",
    "com.eduapp4syria.feedthemonsterSOMALI": "FTM - Somali",
    "com.eduapp4syria.feedthemonsterSAEnglish": "FTM - SouthAfricanEnglish",
    "com.eduapp4syria.feedthemonsterSPANISH": "FTM - Spanish",
    "com.eduapp4syria.feedthemonsterSwahili": "FTM - Swahili",
    "com.eduapp4syria.FeedTheMonsterZULU": "FTM - Zulu",
    "com.eduapp4syria.feedthemonsterAzerbaijani": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterVietnamese": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterJavanese": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterIgbo": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterShona": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterYoruba": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterNdebele": "FTM-AppBucket2",
    "com.eduapp4syria.feedthemonsterTurkishArabic": "FTM-AppBucket3" # need to confirm this bucket
}

# Source: https://docs.google.com/spreadsheets/d/1E19XHn0TRXIGS_sfYp3_nj32w1vMKVVpvIrrTYCx6WE/edit#gid=0 
lang_app_id_dict = {
     'Amharic':'com.eduapp4syria.feedthemonsterAmharic',
     'Kinyarwanda':'com.eduapp4syria.feedthemonsterKinyarwanda',
     'Swahili':'com.eduapp4syria.feedthemonsterSwahili',
     'English (Nigerian)':'com.eduapp4syria.feedthemonsterWAENGLISH',
     'Luganda':'com.eduapp4syria.feedthemonsterLugandan',
     'Shona':'com.eduapp4syria.feedthemonsterShona',
     'Igbo':'com.eduapp4syria.feedthemonsterIgbo',
     'Somali':'com.eduapp4syria.feedthemonsterSOMALI',
     'Hausa':'com.eduapp4syria.feedthemonsterHausa',
     'Oromo':'com.eduapp4syria.feedthemonsterOromo',
     'Wolof':'com.eduapp4syria.feedthemonsterWolof',
     'Yoruba':'com.eduapp4syria.feedthemonsterYoruba',
     'Malagasy':'com.eduapp4syria.feedthemonsterMalagasy',
     'Xhosa':'com.eduapp4syria.feedthemonsterisiXhosa',
     'Venda':'com.eduapp4syria.feedthemonsterVenda',
     'sePedi':'com.eduapp4syria.feedthemonsterSePedi',
     'Tsonga':'com.eduapp4syria.feedthemonsterTsonga',
     'Zulu':'com.eduapp4syria.FeedTheMonsterZULU',
     'Sesotho':'com.eduapp4syria.feedthemonsterSesotho',
     'Ndebele':'com.eduapp4syria.feedthemonsterNdebele',
     'Tswana':'com.eduapp4syria.feedthemonsterTswana',
     'Swati':'com.eduapp4syria.feedthemonsterSiswati',
     'English (South African)':'com.eduapp4syria.feedthemonsterSAEnglish',
     'Afrikaans':'com.eduapp4syria.feedthemonsterAfrikaans',
     'Arabic':'com.eduapp4syria.feedthemonsterArabic',
     'Azerbaijani':'com.eduapp4syria.feedthemonsterAzerbaijani',
     'Farsi':'com.eduapp4syria.feedthemonsterFarsi',
     'Malay':'com.eduapp4syria.feedthemonsterMalay',
     'Turkish':'com.eduapp4syria.feedthemonsterTURKISH',
     'Turkish with Arabic Instructions':'com.eduapp4syria.feedthemonsterTurkishArabic',
     'Tagalog':'com.eduapp4syria.feedthemonsterTagalog',
     'Thai':'com.eduapp4syria.feedthemonsterThai',
     'Javanese':'com.eduapp4syria.feedthemonsterJavanese',
     'Vietnamese':'com.eduapp4syria.feedthemonsterVietnamese',
     'Hindi':'com.eduapp4syria.feedthemonsterHindi',
     'Bangla':'com.eduapp4syria.feedthemonsterBangla',
     'English (Indian)':'com.eduapp4syria.feedthemonsterEnglishIndian',
     'Marathi':'com.eduapp4syria.feedthemonsterMarathi',
     'Nepali':'com.eduapp4syria.feedthemonsterNepali',
     'Ukrainian':'com.eduapp4syria.feedthemonsterUkranian',
     'French':'com.eduapp4syria.feedthemonsterFrench',
     'Spanish':'com.eduapp4syria.feedthemonsterSPANISH',
     'Portuguese (Brazil)':'com.eduapp4syria.feedthemonsterBrazilianPortuguese',
     'Haitian Creole':'com.eduapp4syria.feedthemonsterHatianCreole',
     'English (US)':'com.eduapp4syria.feedthemonsterENGLISH',
     'English (Australian)':'com.eduapp4syria.feedthemonsterENGLISHAUS',
     'Georgian':'com.eduapp4syria.feedthemonsterGeorgian',
     'Pashto':'com.eduapp4syria.feedthemonsterPashto',
     'Kurdish':'com.eduapp4syria.feedthemonsterKurdish',
     'Tajik':'com.eduapp4syria.feedthemonsterTajik'
 }

# Title
st.title('Curious Learning')

# Set up sidebar with date picker
select_date_range = st.sidebar.date_input(
    "Select Date Range:",
    ((pd.to_datetime("today").date() - pd.Timedelta(29, unit='D')),
        (pd.to_datetime("today").date() - pd.Timedelta(1, unit='D'))),
    key = 'date_range'
)

container = st.sidebar.container()
# select_all = st.sidebar.checkbox("Select All App Buckets", value=True)
 
# if select_all:
#     select_bucket = container.multiselect(
#         "Select App Bucket(s)",
#         ["FTM - English", "FTM - Afrikaans", "FTM - AppBucket1", "FTM-AppBucket2",
#             "FTM-AppBucket3", "FTM - French", "FTM - isiXhosa", "FTM - Kinayrwanda",
#             "FTM - Oromo", "FTM - SePedi", "FTM - Somali", "FTM - SouthAfricanEnglish",
#             "FTM - Spanish", "FTM - Swahili", "FTM - Zulu"],
#         ["FTM - English", "FTM - Afrikaans", "FTM - AppBucket1", "FTM-AppBucket2",
#             "FTM-AppBucket3", "FTM - French", "FTM - isiXhosa", "FTM - Kinayrwanda",
#             "FTM - Oromo", "FTM - SePedi", "FTM - Somali", "FTM - SouthAfricanEnglish",
#             "FTM - Spanish", "FTM - Swahili", "FTM - Zulu"],
#         key = 'buckets'
#     )
# else:
#     select_bucket = container.multiselect(
#         "Select App Bucket(s)",
#         ["FTM - English", "FTM - Afrikaans", "FTM - AppBucket1", "FTM-AppBucket2",
#             "FTM-AppBucket3", "FTM - French", "FTM - isiXhosa", "FTM - Kinayrwanda",
#             "FTM - Oromo", "FTM - SePedi", "FTM - Somali", "FTM - SouthAfricanEnglish",
#             "FTM - Spanish", "FTM - Swahili", "FTM - Zulu"],
#         key = 'buckets'
#     )

select_all = st.sidebar.checkbox("Select All Languages", value=True)
langs = sorted(lang_app_id_dict)

if select_all:
    select_language = container.multiselect(
        "Select Language(s):",
        langs,
        ["Afrikaans", "Amharic", "Arabic", "French"],
        #langs,
        key = 'languages'
    )
else:
    select_language = container.multiselect(
        "Select Language(s):",
        langs,
        key = 'languages'
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

# # Get Google Analytics credentials from secrets
# ga_credentials = service_account.Credentials.from_service_account_info(
#     st.secrets["GOOGLE_APPLICATION_CREDENTIALS"]
# )
# # Set global report variables
# dimension_list = ['date', 'country', 'countryId', 'language']
# metric_list = ['active1DayUsers', 'active7DayUsers', 'active28DayUsers',
#                 'newUsers', 'dauPerMau', 'dauPerWau', 'wauPerMau']
# date_range = (st.session_state['date_range'][0].strftime('%Y-%m-%d'),
#                 st.session_state['date_range'][1].strftime('%Y-%m-%d'))

# # Get data for all selected language / app buckets
# main_df = pd.DataFrame()
# for i in st.session_state['buckets']:
#     property_id = app_propertyID_dict[i]
#     ga4_report = ga4.GA4Report(property_id, ga_credentials)
#     report_df = ga4_report.run_report(
#         dimension_list, metric_list,
#         date_ranges = [date_range],
#         #row_limit = 1000,
#         offset_row = 0
#     )
#     report_df = pd.DataFrame(columns = report_df['headers'],
#                                     data = report_df['rows'])
#     report_df.insert(1, "Language/App Bucket", i)
#     main_df = pd.concat([main_df, report_df])

# # Filter main_df
# country_ids_list = countries_df[countries_df.name.isin(st.session_state['country_names'])]['alpha2']
# country_ids_list = [x.upper() for x in country_ids_list]
# main_df = main_df[main_df.countryId.isin(country_ids_list)]


# # Convert date column to datetime, then sort by date
# main_df['date'] = pd.to_datetime(main_df['date'])
# main_df['date'] = main_df['date'].dt.date
# main_df = main_df.sort_values(by=['date'])

# # Convert user count columns to ints
# main_df = main_df.astype({
#     'active1DayUsers': 'int32',
#     'active7DayUsers': 'int32',
#     'active28DayUsers': 'int32',
#     'newUsers': 'int32',
#     'dauPerMau': 'float',
#     'dauPerWau': 'float',
#     'wauPerMau': 'float'
# })

# ---------------------------------------
# BigQuery set up
# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Get BQ data for all selected language / app buckets
bq_df = pd.DataFrame()
for i in st.session_state['languages']:
    temp_df = pd.DataFrame()
    try:
        app_info_id = lang_app_id_dict[i]
    except:
        st.write("Could not find this language in lang_app_id_dict: " + str(i))
        continue
    try:
        bucket = appLang_bucket_dict[app_info_id]
    except:
        st.write(str(i) + " - Could not find this id in appLang_bucket_dict: " + str(app_info_id))
        continue
    try:
        property_id = app_propertyID_dict[bucket]
    except:
        st.write(str(i) + " - Could not find this bucket in app_propertyID_dict: " + str(bucket))
        continue
    try:
        bq_id = app_bqID_dict[bucket]
    except:
        st.write("Could not find this bucket in app_bqID_dict: " + str(bucket))
        continue
    sql_query = f"""
        SELECT event_date, event_name, properties, app_info.id, geo.country
        FROM `{bq_id}.analytics_{property_id}.events_20*`,
        UNNEST (user_properties) as properties
        WHERE parse_date('%y%m%d', _table_suffix) between @start and @end
        and event_name = 'first_open'
        and app_info.id = @appID
    """
    query_parameters = [
        bigquery.ScalarQueryParameter("start", "DATE", st.session_state['date_range'][0]),
        bigquery.ScalarQueryParameter("end", "DATE", st.session_state['date_range'][1]),
        bigquery.ScalarQueryParameter("appID", "STRING", app_info_id)
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters = query_parameters
    )
    rows_raw = client.query(sql_query, job_config=job_config)
    rows = [dict(row) for row in rows_raw]
    temp_df = pd.DataFrame(rows)
    #st.write("temp_df before language col insert:", temp_df)
    temp_df.insert(1, "language", i)
    #st.write("temp_df after language col insert:", temp_df)
    bq_df = pd.concat([bq_df, temp_df], axis=0, ignore_index=True)
    #st.write("bq_df:", bq_df)

#st.write("bq_df before sort:", bq_df.head(n=1000))
bq_df = bq_df.sort_values(by='event_date')
# Convert event_date column to datetime
bq_df['event_date'] = pd.to_datetime(bq_df['event_date'])
bq_df['event_date'] = bq_df['event_date'].dt.date

#st.write("bq_df after sort:", bq_df.head(n=1000))

# --- BQ Learner Acquisition ---
bq_daily_new_users = bq_df[bq_df['event_name'] == 'first_open'].groupby(
    ['event_date'])['event_date'].count().reset_index(name='# New Users')
bq_daily_new_users2 = bq_df[bq_df['event_name'] == 'first_open'].groupby(
    ['event_date', 'language'])['event_date'].count().reset_index(name='# New Users')
bq_daily_new_users['7 Day Rolling Mean'] = bq_daily_new_users['# New Users'].rolling(7).mean()
bq_daily_new_users['30 Day Rolling Mean'] = bq_daily_new_users['# New Users'].rolling(30).mean()
# bq_daily_new_users_fig = px.line(bq_daily_new_users, x='event_date',
#                                     y=['# New Users', '7 Day Rolling Mean', '30 Day Rolling Mean'],
#                                     labels={
#                                         "event_date": "Date (Day)",
#                                         "# New Users": "New Users"},
#                                     title="New User Count by Day")
bq_daily_new_users_fig2 = px.area(bq_daily_new_users2, x='event_date',
                                    y='# New Users',
                                    color="language",
                                    labels={
                                        "event_date": "Date (Day)",
                                        "# New Users": "New Users"},
                                    line_group="language",
                                    title="New User Count by Day")
fig = px.line(bq_daily_new_users,
    x='event_date',
    y=['# New Users', '7 Day Rolling Mean', '30 Day Rolling Mean'],
    labels={"# New Users": "Total New Users"}
)
fig.update_traces(line=dict(width=3, dash='dot'))
bq_daily_new_users_fig2.add_trace(fig.data[0])
bq_daily_new_users_fig2.add_trace(fig.data[1])
bq_daily_new_users_fig2.add_trace(fig.data[2])
# st.plotly_chart(bq_daily_new_users_fig)
st.plotly_chart(bq_daily_new_users_fig2)

# BQ learner acquisition cost by month
bq_lac = bq_df[['event_date', 'event_name', 'id', 'language']]
# result = []
# for i in bq_lac['id']:
#     result.append({j for j in lang_app_id_dict if lang_app_id_dict[j]==i})

# st.write("Result: ", result)
# bq_lac['language'] = result
# st.write("bq_lac", bq_lac.head())
# bq_ref = bq_lac.groupby(['language'], as_index=False)
# st.write(bq_ref.head())
# st.write("bq_lac len = " + str(bq_lac.shape[0]))
# st.write(bq_lac.head(n = 1000))

bq_lac['event_date'] = pd.to_datetime(bq_lac['event_date'])
bq_lac['event_date_year_month'] = bq_lac['event_date'].dt.strftime('%Y-%m')
bq_lac_grouped = bq_lac.groupby(['event_date_year_month','language'], as_index=False)['event_name'].count()
bq_lac_grouped['key'] = bq_lac_grouped['event_date_year_month'] + bq_lac_grouped['language']
# st.write("bq_lac_grouped:")
# st.write(bq_lac_grouped.head(n = 100))

# Create a Google Sheets connection object.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
    ]
)
conn = connect(credentials=credentials)

# Perform SQL query on the Google Sheet.
# Uses st.cache to only rerun when the query changes or after 10 min.
@st.cache(ttl=600)
def run_query(query):
    rows = conn.execute(query, headers=1)
    rows = rows.fetchall()
    return rows

sheet_url = st.secrets["Cost_gsheets_url"]
rows = run_query(f'SELECT * FROM "{sheet_url}"')
sheets_df = pd.DataFrame(columns = ['YearMonth', 'Country', 'Language', 'CostUSD', 'Type', 'Memo'],
                            data = rows)
# Convert date column to datetime, then sort by date
# sheets_df['YearMonth'] = pd.to_datetime(sheets_df['YearMonth'])
# sheets_df['YearMonth'] = sheets_df['YearMonth'].dt.date
# sheets_df = sheets_df.sort_values(by=['YearMonth'])
sheets_df = sheets_df.astype({
    'CostUSD': 'float'
})
sheets_df['CostUSD'] = sheets_df['CostUSD'].round(decimals=2)
# st.write("sheets_df:")
# st.write(sheets_df)

cost_df_grouped = sheets_df.groupby(['YearMonth','Language'], as_index=False)['CostUSD'].sum()
cost_df_grouped['key'] = cost_df_grouped['YearMonth'] + cost_df_grouped['Language']
bq_lac_merged = pd.merge(bq_lac_grouped, cost_df_grouped, how='left', on='key')

# st.write("bq_lac_merged:")
# st.write(bq_lac_merged.head(n=100))

lac = []
for i in bq_lac_merged.index:
    lac.append(bq_lac_merged['CostUSD'][i]/bq_lac_merged['event_name'][i]) # event_name here is actually the # of new users

bq_lac_merged['LAC'] = lac
lac_df = bq_lac_merged[['event_date_year_month', 'language', 'event_name', 'CostUSD', 'LAC']]
lac_df.rename(columns={'event_name': 'New User Count',
                        'event_date_year_month': 'Year-Month',
                        'language': 'Language',
                        'LAC': 'Learner Acquisition Cost (USD)'
                        }, inplace=True)

st.write('Learner Acquisiton Cost Data:')
st.write(lac_df.head(1000))

# --- BQ Learning Acquired ---
st.write('Learning Acquired')
# Read data from levels gsheet
levels_sheet_url = st.secrets["FeedTheMonsterLevels_gsheets_url"]
rows = run_query(f'SELECT * FROM "{levels_sheet_url}"')
levels_sheets_df = pd.DataFrame(columns = ['language', 'current_num_levels', 'num_levels', 'q1', 'q2', 'q3'],
                            data = rows)
st.write(levels_sheets_df)
# Query BQ for quartile data
quart_df = pd.DataFrame()
for lang in st.session_state['languages']:
    try:
        app_id = lang_app_id_dict[lang]
    except:
        st.write("Could not find this language in lang_app_id_dict: " + str(lang))
        continue
    try:
        bucket = appLang_bucket_dict[app_id]
    except:
        st.write("Could not find this id in appLang_bucket_dict: " + str(app_id))
        continue
    try:
        property_id = app_propertyID_dict[bucket]
    except:
        st.write("Could not find this bucket in app_propertyID_dict: " + str(bucket))
        continue
    try:
        bq_id = app_bqID_dict[bucket]
    except:
        st.write("Could not find this bucket in app_bqID_dict: " + str(bucket))
        continue
    try:
        q1 = levels_sheets_df.loc[levels_sheets_df['language'] == lang, 'q1'].item()
        q2 = levels_sheets_df.loc[levels_sheets_df['language'] == lang, 'q2'].item()
        q3 = levels_sheets_df.loc[levels_sheets_df['language'] == lang, 'q3'].item()
    except:
        st.write("Could not find this language in FeedTheMonster-LevelsByLanguage sheet: " + str(lang))
        continue
    sql_query = f"""
        SELECT COUNT(user_pseudo_id) AS num_learners, id, 
            CASE
                WHEN max_level_succeeded <= {q1} THEN 'quartile_1'
                WHEN max_level_succeeded > {q1} AND max_level_succeeded <= {q2} THEN 'quartile_2'
                WHEN max_level_succeeded > {q2} AND max_level_succeeded <= {q3} THEN 'quartile_3'
                ELSE 'quartile_4'
            END AS quartile
        FROM (
            SELECT user_pseudo_id, app_info.id, MAX(CAST(SUBSTR(params.value.string_value, (STRPOS(params.value.string_value, '_') + 1)) as INT64)) AS max_level_succeeded
            FROM `{bq_id}.analytics_{property_id}.events_20*` a,
            UNNEST(a.event_params) AS params
            WHERE parse_date('%y%m%d', _table_suffix) BETWEEN @start AND @end
            AND event_name LIKE 'GamePlay'
            AND params.key = 'action'
            AND params.value.string_value LIKE 'LevelSuccess%'
            AND app_info.id = @appID
            GROUP BY user_pseudo_id, app_info.id
        )
        GROUP BY id, quartile
        ORDER BY id, quartile
    """
    query_parameters = [
        bigquery.ScalarQueryParameter("start", "DATE", st.session_state['date_range'][0]),
        bigquery.ScalarQueryParameter("end", "DATE", st.session_state['date_range'][1]),
        bigquery.ScalarQueryParameter("appID", "STRING", app_id)
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters = query_parameters
    )
    rows_raw = client.query(sql_query, job_config=job_config)
    rows = [dict(row) for row in rows_raw]
    temp_df = pd.DataFrame(rows)
    quart_df = pd.concat([quart_df, temp_df])

st.write(quart_df.head(n=1000))

# # ---------------------------------------
# # Subtitle: Learner Acquisition Metrics
# st.header('Learner Acquisition Metrics')

# # Set up tabs for new user metrics
# tab1, tab2, tab3 = st.tabs(["New Users by Day", "New Users by Week", "New Users by Month"])

# # Calculate number of unique profiles
# # def calc_num_profiles():

# #     return null

# # --- New Users by Day ---
# daily_new_users_df = main_df[['date','newUsers']]

# daily_new_users_df = daily_new_users_df.groupby(['date']).sum().reset_index()
# daily_new_users_df['7 Day Rolling Mean'] = daily_new_users_df['newUsers'].rolling(7).mean()
# daily_new_users_df['30 Day Rolling Mean'] = daily_new_users_df['newUsers'].rolling(30).mean()
# daily_new_users_df.rename(columns={'newUsers': 'Daily New Users'})

# daily_new_users_fig = px.line(daily_new_users_df, x='date',
#                                 y=['newUsers', '7 Day Rolling Mean', '30 Day Rolling Mean'],
#                                 title="New Users by Day",
#                                 labels={"value": "New Users", "date": "Date",
#                                     "newUsers": "Daily New Users"})
# tab1.plotly_chart(daily_new_users_fig)

# # --- New Users by Week ---
# weekly_new_users_df = main_df[['date','newUsers']]

# weekly_new_users_df['date'] = pd.to_datetime(weekly_new_users_df['date'])
# weekly_new_users_df.set_index('date', inplace=True)
# weekly_new_users_df = weekly_new_users_df.resample('W').sum()

# weekly_new_users_fig = px.line(weekly_new_users_df,
#                                 y=['newUsers'],
#                                 title="New Users by Week",
#                                 labels={"value": "New Users", "date": "Date"},
#                                 markers=True)
# tab2.plotly_chart(weekly_new_users_fig)

# # --- New Users by Month ---
# monthly_new_users_df = main_df[['date','newUsers']]

# monthly_new_users_df['date'] = pd.to_datetime(monthly_new_users_df['date'])
# monthly_new_users_df.set_index('date', inplace=True)
# monthly_new_users_df = monthly_new_users_df.resample('M').sum()

# monthly_new_users_fig = px.line(monthly_new_users_df,
#                                 y=['newUsers'],
#                                 title="New Users by Month",
#                                 labels={"value": "New Users", "date": "Date"},
#                                 markers=True)
# tab3.plotly_chart(monthly_new_users_fig)


# tab4, tab5 = st.tabs(["New Users by Country", "New Users by Language"])

# # --- New Users by Country ---
# new_users_by_country_df = main_df[
#     ['country', 'newUsers']
# ]

# new_users_by_country_df = new_users_by_country_df.groupby(['country']).sum().reset_index()

# country_fig2 = px.choropleth(new_users_by_country_df, locations='country',
#                                     color='newUsers',
#                                     color_continuous_scale='Emrld',
#                                     locationmode='country names',
#                                     title='New Users by Country',
#                                     labels={'newUsers':'New Users'})
# country_fig2.update_layout(geo=dict(bgcolor= 'rgba(0,0,0,0)'))

# tab4.plotly_chart(country_fig2)

# # --- New Users by Language ---
# new_users_by_lang_df = main_df[
#     ['language', 'newUsers']
# ]

# new_users_by_lang_df = new_users_by_lang_df.groupby(['language']).sum().reset_index()
# # new_users_by_lang_fig = px.pie(new_users_by_lang_df, values='newUsers', names='language',
# #                                 title="New Users by Language")
# new_users_by_lang_fig = px.bar(new_users_by_lang_df, x='language',
#                                  y='newUsers',
#                                  title="New Users by Language")
# tab5.markdown("*Note that 'language' below is the language setting of the user's browser or device. e.g. English*")
# tab5.plotly_chart(new_users_by_lang_fig)

# # --- Learner Acquisition Cost ---



# # Subtitle: Learner Engagement Metrics
# st.markdown("***")
# st.header('Learner Engagement Metrics')

# # --- Active Users by Country ---
# users_by_country_df = main_df[
#     ['country', 'active1DayUsers']
# ]

# users_by_country_df = users_by_country_df.groupby(['country']).sum().reset_index()

# country_fig = px.choropleth(users_by_country_df, locations='country',
#                                     color='active1DayUsers',
#                                     color_continuous_scale='Emrld',
#                                     locationmode='country names',
#                                     title='Active Users by Country',
#                                     labels={'active1DayUsers':'Active Users'})
# country_fig.update_layout(geo=dict(bgcolor= 'rgba(0,0,0,0)'))
# st.plotly_chart(country_fig)

# # --- Users Stickiness ---
# user_stickiness_df = main_df[
#     ['date', 'dauPerMau', 'dauPerWau', 'wauPerMau']
# ]
# user_stickiness_df['dauPerMau'] = round(user_stickiness_df['dauPerMau'], 1)
# user_stickiness_df['dauPerWau'] = round(user_stickiness_df['dauPerWau'], 1)
# user_stickiness_df['wauPerMau'] = round(user_stickiness_df['wauPerMau'], 1)

# user_stickiness_df = user_stickiness_df.groupby(['date']).sum().reset_index()

# user_stickiness_fig = px.line(user_stickiness_df, x='date',
#                             y=['dauPerMau', 'dauPerWau', 'wauPerMau'],
#                                 title="User Stickiness",
#                                 labels={'value': '%', 'date': 'Date'})
# st.plotly_chart(user_stickiness_fig)

# # --- Active Users by Day ---
# active_users_df = main_df[
#     ['date', 'active1DayUsers', 'active7DayUsers', 'active28DayUsers']
# ]

# # Group data by day and plot as timeseries chart
# active_users_df = active_users_df.groupby(['date']).sum().reset_index()

# active_users_fig = px.line(active_users_df, x='date',
#                             y=['active1DayUsers', 'active7DayUsers', 'active28DayUsers'],
#                                 title="Active Users by Day",
#                                 labels={"value": "Active Users",
#                                     "date": "Date", "variable": "Trailing"})
# st.plotly_chart(active_users_fig)


# # --- Raw Data ---
# st.write(main_df)
#   # -----------------------------------------
