from .scan_result import ScanResult


class BigQueryScanResult(ScanResult):
    def get_table_name(self):
        return self.table_name.strip('`')