import json
import os

from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
import streamlit as st


KEY_PATH = os.environ['BQ_SA_KEY_JSON']
PROJECT_ID = os.environ['PROJECT_ID']
MEASUREMENTS_TABLE = os.environ['MEASUREMENTS_TABLE']

@st.cache
def load_data(sql):
    credentials = service_account.Credentials.from_service_account_file(
        KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID, credentials=credentials)

    return df


st.set_page_config(layout='wide')

execution_datetimes_query = f'''
SELECT
  DISTINCT execution_datetime
FROM
  `{MEASUREMENTS_TABLE}`
ORDER BY
  execution_datetime DESC
'''
execution_datetimes = load_data(execution_datetimes_query)
execution_datetime = st.sidebar.selectbox(
  'execution datetime',
  execution_datetimes['execution_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
)

tables_query = f'''
SELECT
  DISTINCT table_name
FROM
  `{MEASUREMENTS_TABLE}`
ORDER BY
  table_name DESC
'''
tables = load_data(tables_query)
table = st.sidebar.selectbox(
  'table',
  tables['table_name']
)

measurements_query = f"""
SELECT
  * EXCEPT(table_name, execution_datetime)
FROM
  `{MEASUREMENTS_TABLE}`
WHERE
  execution_datetime = '{execution_datetime}'
  AND table_name = '{table}'
"""
profile = load_data(measurements_query)
overview = profile[profile['column_name'].isnull() & ~(profile['metric'].isin(['schema', 'schema_change']))]
overview_table = overview[['metric', 'value']]
st.header('Overview')
st.write(overview_table.style.hide(axis='index').to_html(), unsafe_allow_html=True)
measurements = profile[profile['column_name'].notnull()]
measurements_table = measurements.pivot(index='column_name', columns='metric', values='value')
st.header('Measurements')
st.table(measurements_table)

schema_json = profile[profile['metric'] == 'schema']['value'].iloc[0]
schema = json.loads(schema_json)
schema_table = pd.DataFrame(schema)[['name', 'type', 'nullable']]
schema_table['nullable'] = schema_table['nullable'].apply(lambda x: 'Yes' if x else 'No')
schema_change = json.loads(profile[profile['metric'] == 'schema_change']['value'].iloc[0])

st.header('Schema')
if schema_change['schema_changed']:
  latest_schema_query = f"""
  SELECT
    * EXCEPT(table_name, execution_datetime)
  FROM
    `{MEASUREMENTS_TABLE}`
  WHERE
    execution_datetime = '{schema_change["latest_execution_dt"]}'
    AND table_name = '{table}'
    AND metric = 'schema'
  """
  latest_schema_record = load_data(latest_schema_query)
  latest_schema_json = latest_schema_record['value'].iloc[0]
  latest_schema = json.loads(latest_schema_json)
  latest_schema_table = pd.DataFrame(latest_schema)[['name', 'type', 'nullable']]

  st.error('Schema changed!')
  col1, col2 = st.columns(2)
  with col1:
    st.subheader('current schema')
    st.write(schema_table.style.hide(axis='index').to_html(), unsafe_allow_html=True)

  with col2:
    st.subheader(f'schema at {schema_change["latest_execution_dt"]}')
    st.write(latest_schema_table.style.hide(axis='index').to_html(), unsafe_allow_html=True)
else:
  st.subheader('schema')
  st.write(schema_table.style.hide(axis='index').to_html(), unsafe_allow_html=True)
