import json

from sodasql.scan.measurement import Measurement

class ScanResult:
    def __init__(self, scan_result, table_name, execution_datetime):
        self.scan_result= scan_result
        self.schema_check_result = None
        self.table_name = table_name
        self.execution_datetime = execution_datetime.strftime('%Y-%m-%d %H:%M:%S')

    def add_schema_check_result(self, schema_check_result):
        self.scan_result.measurements.append(
            Measurement(
                metric='schema_change',
                value=schema_check_result,
            )
        )

    def to_dict(self):
        return self.scan_result.to_dict()

    def is_passed(self):
        return self.scan_result.is_passed()

    def get_measurement(self, metric_type, columne_name=None):
        return self.scan_result.get_measurement(
            metric_type=metric_type,
            column_name=columne_name,
        )

    def get_execution_datetime(self):
        return self.execution_datetime

    def get_table_name(self):
        return self.table_name

    def get_schema(self):
        return self.get_measurement('schema').value
