from datetime import datetime
from scan_result import BigQueryScanResult

from sodasql.scan.scan_builder import ScanBuilder


SCAN_RESULT_CLASS = {
    'bigquery': BigQueryScanResult,
}


class Scanner:
    def __init__(self, warehouse_yml_file):
        self.warehouse_yml_file = warehouse_yml_file
        self.scan_result = None

    def _execute(self, execution_datetime):
        scan_result = self.scan.execute()
        return SCAN_RESULT_CLASS[self.scan.warehouse.dialect.type](scan_result, self.scan.qualified_table_name, execution_datetime)

    def run_soda_scan(self, scan_yml_file, execution_datetime=None):
        scan_builder = ScanBuilder()
        scan_builder.warehouse_yml_file = self.warehouse_yml_file
        scan_builder.scan_yml_file = scan_yml_file
        self.scan = scan_builder.build()

        if execution_datetime is None:
            execution_datetime_ = datetime.now()
        else:
            execution_datetime_ = datetime.strptime(execution_datetime, '%Y-%m-%d %H:%M:%S')

        return self._execute(execution_datetime_)
