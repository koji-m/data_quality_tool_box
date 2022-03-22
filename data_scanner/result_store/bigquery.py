import json

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq


class BigQuery:

    LATEST_SCHEMA_QUERY = """
    SELECT 
        value,
        execution_datetime
    FROM
        `{measurements_table}`
    WHERE
        table_name = '{table_name}'
        AND metric = 'schema'
    QUALIFY
        ROW_NUMBER() OVER (ORDER BY execution_datetime DESC) = 1
    """

    MEASUREMENTS_LOAD_JOB_CONFIG = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("metric", "STRING"),
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("column_name", "STRING"),
            bigquery.SchemaField("execution_datetime", "DATETIME"),
        ]
    )

    TEST_RESULTS_LOAD_JOB_CONFIG = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("expression", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("column_name", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("passed", "BOOLEAN"),
            bigquery.SchemaField("skipped", "BOOLEAN"),
            bigquery.SchemaField("row_count", "INTEGER"),
            bigquery.SchemaField("expression_result", "FLOAT"),
            bigquery.SchemaField("invalid_percentage", "FLOAT"),
            bigquery.SchemaField("execution_datetime", "DATETIME"),
        ]
    )

    def __init__(self, key_path, project_id, measurements_table, test_result_table):
        self.key_path = key_path
        self.credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.project_id = project_id
        self.measurements_table = measurements_table
        self.test_result_table = test_result_table

    def credentials(self):
        return self.credentials

    def project_id(self):
        return self.project_id

    def get_latest_schema(self, table_name):
        latest_schemas_df = pandas_gbq.read_gbq(
            self.LATEST_SCHEMA_QUERY.format(
                table_name=table_name,
                measurements_table=self.measurements_table,
            ),
            project_id=self.project_id,
            credentials=self.credentials,
        )
        if len(latest_schemas_df):
            return (
                json.loads(latest_schemas_df['value'].iloc[0]),
                latest_schemas_df['execution_datetime'].iloc[0].strftime('%Y-%m-%d %H:%M:%S'),
            )
        else:
            return None

    def check_schema_change(self, schema, table_name):
        latest_schema_, latest_execution_datetime = self.get_latest_schema(table_name)
        changed = None
        if latest_schema_:
            current_schema = sorted(schema, key=lambda x: x['name'])
            latest_schema = sorted(latest_schema_, key=lambda x: x['name'])
            if current_schema == latest_schema:
                changed = False
            else:
                changed = True
        else:
            changed = False

        return {
            'schema_changed': changed,
            'latest_execution_dt': latest_execution_datetime
        }

    @staticmethod
    def convert(scan_result):
        res = scan_result.to_dict()
        table_name = scan_result.get_table_name()
        execution_datetime = scan_result.get_execution_datetime()
        measurements = BigQuery.convert_measurements(res, table_name, execution_datetime)
        test_results = BigQuery.convert_test_results(res, table_name, execution_datetime)
        return (measurements, test_results)

    @staticmethod
    def convert_measurements(res, table_name, execution_datetime):
        measurement_records = []
        for measurement in res.get('measurements', []):
            value = json.dumps(measurement['value'])
            measurement_records.append({
                'metric': measurement['metric'],
                'value': value,
                'table_name': table_name,
                'column_name': measurement.get('columnName'),
                'execution_datetime': execution_datetime,
            })

        return measurement_records

    def convert_test_results(res, table_name, execution_datetime):
        test_result_records = []
        for test_result in res.get('testResults', []):
            test_result_records.append({
                'id': test_result['id'],
                'title': test_result['title'],
                'description': test_result.get('description'),
                'expression': test_result.get('expression'),
                'table_name': table_name,
                'column_name': test_result.get('columnName'),
                'source': test_result.get('source'),
                'passed': test_result['passed'],
                'skipped': test_result['skipped'],
                'row_count': test_result['values'].get('row_count'),
                'expression_result': test_result['values'].get('expression_result'),
                'invalid_percentage': test_result['values'].get('invalid_percentage'),
                'execution_datetime': execution_datetime,
            })
        
        return test_result_records

    def load_measurements(self, measurement_records, client):
        load_job = client.load_table_from_json(
            measurement_records,
            self.measurements_table,
            location='US',
            job_config=self.MEASUREMENTS_LOAD_JOB_CONFIG,
        )

        load_job.result()
        destination_table = client.get_table(self.measurements_table)
        print(f"Loaded {destination_table.num_rows} rows.")

    def load_test_results(self, test_results, client):
        load_job = client.load_table_from_json(
            test_results,
            self.test_result_table,
            location='US',
            job_config=self.TEST_RESULTS_LOAD_JOB_CONFIG,
        )

        load_job.result()
        destination_table = client.get_table(self.test_result_table)
        print(f"Loaded {destination_table.num_rows} rows.")

    def load(self, scan_result):
        measurements, test_results = BigQuery.convert(scan_result)
        client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.load_measurements(measurements, client)
        self.load_test_results(test_results, client)
