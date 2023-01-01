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
from millify import millify

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

# @st.experimental_memo
# def get_campaign_data():
#     campaign_sheet_url = st.secrets["Campaign_gsheets_url"]
#     campaign_rows = run_query(f'SELECT * FROM "{campaign_sheet_url}"')
#     campaign_data = pd.DataFrame(columns = ['Campaign Name', 'Language', 'Country', 'Start Date', 'End Date', 'Total Cost (USD)'],
#                             data = campaign_rows)
#     campaign_data['Start Date'] = (pd.to_datetime(campaign_data['Start Date'])).dt.date
#     campaign_data['End Date'] = (pd.to_datetime(campaign_data['End Date'])
#                                     + pd.DateOffset(months=1) - pd.Timedelta(1, unit='D')).dt.date
#     campaign_data = campaign_data.astype({
#         'Total Cost (USD)': 'float'
#     })
#     return campaign_data

@st.experimental_memo
def get_user_data(start_date, end_date, apps, countries):
    start = start_date.strftime('%Y%m%d')
    end = end_date.strftime('%Y%m%d')
    sql_query = f"""
        SELECT * FROM `dataexploration-193817.user_data.ftm_users`
        WHERE LA_date BETWEEN @start AND @end
        AND app_id IN UNNEST(@apps)
        AND country IN UNNEST(@countries)
    """
    query_parameters = [
        bigquery.ScalarQueryParameter("start", "STRING", start),
        bigquery.ScalarQueryParameter("end", "STRING", end),
        bigquery.ArrayQueryParameter("apps", "STRING", apps),
        bigquery.ArrayQueryParameter("countries", "STRING", countries)
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters = query_parameters
    )
    rows_raw = client.query(sql_query, job_config = job_config)
    rows = [dict(row) for row in rows_raw]
    df = pd.DataFrame(rows)
    df['LA_date'] = (pd.to_datetime(df['LA_date'])).dt.date
    df['max_lvl_date'] = (pd.to_datetime(df['max_lvl_date'])).dt.date
    return df

@st.experimental_memo
def get_apps_data():
    apps_sheet_url = st.secrets["ftm_apps_gsheets_url"]
    apps_rows = run_query(f'SELECT app_id, language, bq_property_id, bq_project_id, total_lvls FROM "{apps_sheet_url}"')
    apps_data = pd.DataFrame(columns = ['app_id', 'language', 'bq_property_id', 'bq_project_id', 'total_lvls'],
        data = apps_rows)
    return apps_data

@st.experimental_memo
def get_ra_segments(campaign_data, app_data, user_data):
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
    res['rac'] = campaign_data['Total Cost (USD)'][0] * res['la_perc'] / (res['ra'] * res['la'].sum())
    return res

@st.experimental_memo
def get_daily_activity(start_date, apps, countries, bq_ids, property_ids):
    res = pd.DataFrame()
    for i in range(len(bq_ids)):
        sql_query = f"""
            SELECT event_date, COUNT(event_name) AS levels_played FROM `{bq_ids[i]}.analytics_{property_ids[i]}.events_20*`,
            UNNEST(event_params) AS params
            WHERE PARSE_DATE('%y%m%d', _table_suffix) BETWEEN @start AND @end
            AND app_info.id IN UNNEST(@apps)
            AND geo.country IN UNNEST(@countries)
            AND event_name LIKE 'GamePlay'
            AND params.key = 'action'
            AND (params.value.string_value LIKE '%LevelSuccess%'
            OR params.value.string_value LIKE '%LevelFail%')
            GROUP BY event_date
            ORDER BY event_date 
        """
        query_parameters = [
            bigquery.ScalarQueryParameter("start", "DATE", start_date),
            bigquery.ScalarQueryParameter("end", "DATE", pd.to_datetime("today").date()-pd.Timedelta(1, unit='D')),
            bigquery.ArrayQueryParameter("apps", "STRING", apps),
            bigquery.ArrayQueryParameter("countries", "STRING", countries)
            # bigquery.ScalarQueryParameter("bq_id", "STRING", bq_id),
            # bigquery.ScalarQueryParameter("property_id", "STRING", property_id)
        ]
        job_config = bigquery.QueryJobConfig(
            query_parameters = query_parameters
        )
        st.write(sql_query)
        rows_raw = client.query(sql_query, job_config = job_config)
        rows = [dict(row) for row in rows_raw]
        df = pd.DataFrame(rows)
        df['event_date'] = (pd.to_datetime(df['event_date'])).dt.date
        res = pd.concat([res, df])
    return res

# --- UI ---
st.title('Manual Analysis')
expander = st.expander('Definitions')
# CSS to inject contained in a string
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """
# Inject CSS with Markdown
st.markdown(hide_table_row_index, unsafe_allow_html=True)
def_df = pd.DataFrame(
    [
        ['LA', 'Learner Acquisition', 'The number of users that have completed at least one FTM level'],
        ['LAC', 'Learner Acquisition Cost', 'The cost (USD) of acquiring one learner'],
        ['RA', 'Reading Acquisition', 'The average percentage of FTM levels completed per learner'],
        ['RAC', 'Reading Acquisition Cost', 'The cost (USD) of acquiring the average amount of reading per learner']
    ],
    columns=['Acronym', 'Name', 'Definition']
)
expander.table(def_df)

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

# SET VARIABLES
ftm_apps = get_apps_data()
start_date = st.session_state['date_range'][0]
end_date = st.session_state['date_range'][1]
languages = st.session_state['languages']
apps = set()
bq_ids = set()
property_ids = set()
for l in languages:
    apps.add(ftm_apps.loc[ftm_apps['language'] == l, 'app_id'].item())
    bq_ids.add(ftm_apps.loc[ftm_apps['language'] == l, 'bq_project_id'].item())
    property_ids.add(ftm_apps.loc[ftm_apps['language'] == l, 'bq_property_id'].item())
apps = list(apps)
bq_ids = list(bq_ids)
property_ids = list(property_ids)
countries = st.session_state['countries']
users_df = get_user_data(start_date, end_date, apps, countries)

# DAILY READING ACTIVITY
st.write('bq_ids', bq_ids)
daily_activity = get_daily_activity(start_date, apps, countries, bq_ids, property_ids)
col1, col2 = st.columns(2)
col1.metric('Total LA', millify(str(len(users_df))))
col2.metric('Total Levels Played', millify(daily_activity['levels_played'].sum()))
# daily_activity_fig = px.imshow(daily_activity)
daily_activity_fig = px.bar(daily_activity,
    x='event_date',
    y='levels_played',
    labels={
        'event_date': 'Date',
        'levels_played': '# Levels Played'
    },
    title='Daily Reading Activity'
)
st.plotly_chart(daily_activity_fig)

# DAILY LEARNERS ACQUIRED
daily_la = users_df.groupby(['LA_date'])['user_pseudo_id'].count().reset_index(name='Learners Acquired')
daily_la['7 Day Rolling Mean'] = daily_la['Learners Acquired'].rolling(7).mean()
daily_la['30 Day Rolling Mean'] = daily_la['Learners Acquired'].rolling(30).mean()
daily_la_fig = px.line(daily_la,
    x='LA_date',
    y='Learners Acquired',
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

# DAILY READING ACQUIRED

# READING ACQUISITION DECILES
# ra_segs = get_ra_segments(ftm_campaigns, ftm_apps, users_df)
# ra_segs_fig = px.bar(ra_segs,
#     x='seg',
#     y='la_perc',
#     hover_data=['rac', 'la'],
#     labels={
#         'la_perc': '% LA',
#         'seg': 'RA Decile',
#         'rac': 'RAC (USD)',
#         'la': 'LA'
#     },
#     title='LA by RA Decile' 
# )
# st.plotly_chart(ra_segs_fig)