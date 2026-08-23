import csv
import os

from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.sdk.trace.export import SpanExportResult


class CSVSpanExporter(SpanExporter):
    def __init__(self, path: str):
        self._file_exists = os.path.exists(path)
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._file = open(path, 'a', newline='')
        self._writer = None

    def export(self, spans) -> SpanExportResult:
        try:
            for span in spans:
                row = {
                    'trace_id': format(span.context.trace_id, '032x'),
                    'span_id': format(span.context.span_id, '016x'),
                    'parent_span_id': (format(span.parent.span_id, '016x') if span.parent else ''),
                    'name': span.name,
                    'duration_s': (span.end_time - span.start_time) / 1e9,
                    **dict(span.resource.attributes),
                    **dict(span.attributes),
                }
                if self._writer is None:
                    self._writer = csv.DictWriter(self._file, fieldnames=list(row.keys()))
                    if not self._file_exists:
                        self._writer.writeheader()
                self._writer.writerow(row)
            self._file.flush()
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        self._file.close()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        self._file.flush()
        return True
