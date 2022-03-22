import os

from google.oauth2 import service_account
import pandas_gbq
import streamlit as st


KEY_PATH = os.environ['BQ_SA_KEY_JSON']
PROJECT_ID = os.environ['PROJECT_ID']
TEST_RESULTS_TABLE = os.environ['TEST_RESULTS_TABLE']

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
  `{TEST_RESULTS_TABLE}`
ORDER BY
  execution_datetime DESC
'''
execution_datetimes = load_data(execution_datetimes_query)
execution_datetime = st.selectbox(
  'execution datetime',
  execution_datetimes['execution_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
)

test_results_query = f"""
WITH
test_results AS (
  SELECT
    *
  FROM
    `{TEST_RESULTS_TABLE}` 
  WHERE
    execution_datetime = '{execution_datetime}'
)
SELECT
  count(*) AS num_tests,
  (SELECT COUNT(*) FROM test_results WHERE passed = false) AS num_failed,
  (SELECT COUNT(*) FROM test_results WHERE skipped = true) AS num_skipped,
  CAST((SELECT COUNT(*) FROM test_results WHERE passed = true) / count(*) * 100 AS INT) AS success_rate,
FROM
  test_results
"""
test_results_query_ = f"SELECT * FROM `{TEST_RESULTS_TABLE}` WHERE execution_datetime = '{execution_datetime}'"
test_results = load_data(test_results_query_)

num_tests = len(test_results)
num_failed = (test_results['passed'] == False).sum()
num_skipped = (test_results['skipped'] == True).sum()
success_rate = int((test_results['passed'] == True).sum() / num_tests * 100)

col1, col2, col3, col4 = st.columns(4)
col1.metric('Tests', num_tests)
col2.metric('Failed', num_failed)
col3.metric('Skipped', num_skipped)
col4.metric('Success', f'{success_rate}%')

if num_failed > 0:
  st.subheader('Failed Tests')
  failed_tests = test_results[test_results['passed'] == False]
  failed_tests_table = failed_tests[['table_name', 'column_name', 'title', 'expression', 'expression_result']]
  st.write(failed_tests_table.style.hide(axis='index').to_html(), unsafe_allow_html=True)
