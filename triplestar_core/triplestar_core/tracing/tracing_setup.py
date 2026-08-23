# tracing_setup.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from triplestar_core.tracing.csv_exporter import CSVSpanExporter


def setup_tracer(out_path):
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(CSVSpanExporter(out_path)))
    trace.set_tracer_provider(provider)
    return trace.get_tracer('triplestar_bench')
