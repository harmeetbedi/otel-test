import datetime
import logging
import time

from opentelemetry import _logs
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk._logs import LoggingHandler, LoggerProvider
from opentelemetry.sdk._logs._internal.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ALWAYS_OFF
from opentelemetry.trace import SpanKind


# todo: is log formatting done even if trace is discarded by sampling ?
# todo: semantics of events
# todo: How will grpc errors show up ? e.g. timeout. how will it show up ? Python grpc error mimic.
# todo: what is stacktrace. It may be very expensive. Should be enabled in a selectable manner. Server side control ??


# setup app tracing to send traces to otel collector
def configure_tracing(server):
    endpoint = f"{server}/v1/traces"
    trace_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, timeout=100))
    tracer_provider = TracerProvider(resource=Resource(attributes={SERVICE_NAME: "harmeet-trace-test_2"}), sampler=ALWAYS_ON)
    trace.set_tracer_provider(tracer_provider)
    tracer_provider.add_span_processor(trace_processor)
    _tracer = trace.get_tracer("harmeet-trace-test")
    return _tracer


# setup app to send logs to otel collector
def configure_logging(server):
    endpoint = f"{server}/v1/logs"
    log_processor = BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, timeout=100))
    logger_provider = LoggerProvider(resource=Resource(attributes={SERVICE_NAME: "harmeet-log-test"}))
    _logs.set_logger_provider(logger_provider)
    logger_provider.add_log_record_processor(log_processor)

    # Automatically inject trace context into logs
    instrumentation = LoggingInstrumentor()
    instrumentation.instrument()

    # Setup logger
    _logger = logging.getLogger("basic_otlp_test")
    # Set up logging to use the OpenTelemetry logging integration
    handler = LoggingHandler()
    _logger.addHandler(handler)
    return _logger

class DynamicLogLevelHandler(LoggingHandler):
    def __init__(
            self,
            level=logging.NOTSET,
            logger_provider=None,

    ) -> None:
        super().__init__(level=level, logger_provider=logger_provider)

    def emit(self, record: logging.LogRecord) -> None:
        rec = self._translate(record)
        is_sampled = rec.trace_flags > 0
        print(f"sampled={is_sampled}, severity={rec.severity_number}, {rec.severity_text}")
        if is_sampled:
            rec.severity_number = 40
            rec.severity_text = "ERROR"
        super().emit(record=record)

# log messages, with delays and console output
def log_msg(_logger, msg):
    time.sleep(.5)
    print(f"{datetime.datetime.now()} {msg}")
    _logger.error(msg)
    #_logger.debug(f"debug {msg}")
    time.sleep(.5)

def gen_linked_span_context(_tracer, _logger):
    with _tracer.start_as_current_span("linked-service") as span:
        log_msg(_logger, "Starting span linked-service")
        span_context = span.get_span_context()
        span.set_attribute("attr1", "hello1")
        span.set_attribute("attr2", "world1")
        span.add_event("event1a")
        print(f"linked_span > trace_id={hex(span_context.trace_id)}, span_id={hex(span_context.span_id)}")
        return span.get_span_context()

def gen_test_data(_tracer, _logger, linked_span_context):
    with _tracer.start_as_current_span("service_1",links=[trace.Link(context=linked_span_context)]) as span:
        log_msg(_logger, "Started span service_1")

        try:
            raise ValueError('Invalid input')
        except ValueError as e:
            span.record_exception(e)
        # Get trace context
        span_context = span.get_span_context()
        is_sampled = span_context.trace_flags > 0
        print(f"span > trace_id={hex(span_context.trace_id)}, span_id={hex(span_context.span_id)}, sampled={is_sampled}")
        span.set_attribute("attr1", "hello1")
        span.set_attribute("attr2", "world1")
        span.add_event("event1a")
        log_msg(_logger, "added event to service1")
        context = trace.set_span_in_context(span)
        log_msg(_logger, "starting child_span")
        with _tracer.start_as_current_span("service2", context=context):
            log_msg(_logger, "Starting span service2")
            child = trace.get_current_span()
            child_span_context = span.get_span_context()
            print(f"span > trace_id={hex(child_span_context.trace_id)}, span_id={hex(child_span_context.span_id)}")
            child.set_attribute("attr1", "hello2")
            child.set_attribute("attr2", "world2")
            child.add_event("event2a")
            attributes = {"attr1": "event_hello1", "attr2": "event_hello2"}
            child.add_event("event2b", attributes=attributes)
            log_msg(_logger, "added events to service2")
            log_msg(_logger, "Ending span service2")
        trace.set_span_in_context(span)
        log_msg(_logger, "back in parent span")
        span.add_event("event1b")
        log_msg(_logger, "added event to service1")
        log_msg(_logger, "Ending span service1")


if __name__ == "__main__":
    vm_endpoint = "http://34.72.18.251:4318"
    tracer = configure_tracing(vm_endpoint)
    logger = configure_logging(vm_endpoint)
    #log_msg(logger, "Log_Test_Message")
    linked_span_context = gen_linked_span_context(tracer, logger)
    gen_test_data(tracer, logger, linked_span_context)
    print("Done")

