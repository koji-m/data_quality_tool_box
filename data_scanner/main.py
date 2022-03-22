import os
import sys

from result_store import BigQuery
from scanner import Scanner


KEY_PATH = os.environ['BQ_SA_KEY_JSON']
PROJECT_ID = os.environ['PROJECT_ID']
MEASUREMENTS_TABLE = os.environ['MEASUREMENTS_TABLE']
TEST_RESULTS_TABLE = os.environ['TEST_RESULTS_TABLE']


if __name__ == '__main__':
    warehouse_yml_file = sys.argv[1]
    scan_yml_file = sys.argv[2]
    scanner = Scanner(warehouse_yml_file)
    scan_result = scanner.run_soda_scan(scan_yml_file)

    passed = scan_result.is_passed()
    result_store = BigQuery(
        key_path=KEY_PATH,
        project_id=PROJECT_ID,
        measurements_table=MEASUREMENTS_TABLE,
        test_result_table=TEST_RESULTS_TABLE,
    )
    schema_check_result = result_store.check_schema_change(
        scan_result.get_schema(),
        scan_result.get_table_name(),
    )

    scan_result.add_schema_check_result(schema_check_result)

    result_store.load(scan_result)

    scan_summary = {
        'is_passed': passed,
        'schema_check_result': {
            'execution_dt': scan_result.get_execution_datetime(),
            **schema_check_result,
        }
    }

    print(scan_summary)
