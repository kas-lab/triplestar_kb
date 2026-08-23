import os

from opentelemetry import trace


def get_tracer():
    if os.environ.get('TRIPLESTAR_ENABLE_TRACING') == '1':
        from triplestar_core.tracing.tracing_setup import setup_tracer

        out_path = os.environ.get('TRIPLESTAR_TRACE_OUTPUT_PATH', '/tmp/triplestar_trace.csv')
        return setup_tracer(out_path)
    # if no tracer setup is performed, the tracer will do a no-op (trace nothing)
    return trace.get_tracer('triplestar_bench')
