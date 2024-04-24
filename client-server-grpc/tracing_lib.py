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
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.export import PeriodicExportingMetricReader
from opentelemetry.metrics import Observation, CallbackOptions
from opentelemetry.instrumentation import grpc as grpc_instrumentation

import helloworld_pb2
import helloworld_pb2_grpc
from concurrent import futures
import grpc

# todo: is log formatting done even if trace is discarded by sampling ?
# todo: semantics of events
# todo: How will grpc errors show up ? e.g. timeout. how will it show up ? Python grpc error mimic.
# todo: what is stacktrace. It may be very expensive. Should be enabled in a selectable manner. Server side control ??

OTEL_COLLECTOR = "34.72.18.251"


def configure_metrics(server=OTEL_COLLECTOR, service_name="metrics_test"):
    exporter = OTLPMetricExporter(endpoint=f"http://{server}:4317", insecure=True)
    # reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)
    reader = PeriodicExportingMetricReader(exporter)
    provider = MeterProvider(metric_readers=[reader], resource=Resource(attributes={SERVICE_NAME: service_name}))
    metrics.set_meter_provider(provider)
    return metrics
    # meter = metrics.get_meter(service_name)
    # return exporter,meter


# setup app tracing to send traces to otel collector
def configure_tracing(server=OTEL_COLLECTOR, service_name="trace_test", tenant="test_tenant"):
    endpoint = f"http://{server}:4318/v1/traces"
    trace_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, timeout=100))
    tracer_provider = TracerProvider(resource=Resource(attributes={SERVICE_NAME: service_name, "tenant_id": tenant}), sampler=ALWAYS_ON)
    trace.set_tracer_provider(tracer_provider)
    tracer_provider.add_span_processor(trace_processor)
    _tracer = trace.get_tracer("harmeet-trace-test")
    return _tracer


# setup app to send logs to otel collector
def configure_logging(server=OTEL_COLLECTOR, service_name="log_test", tenant="test_tenant"):
    endpoint = f"http://{server}:4318/v1/logs"
    log_processor = BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, timeout=100))
    logger_provider = LoggerProvider(resource=Resource(attributes={SERVICE_NAME: "harmeet-log-test", "tenant_id": tenant}))
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
    time.sleep(1)
    print(f"{datetime.datetime.now()} {msg}")
    _logger.error(msg)
    _logger.debug(f"debug {msg}")
    time.sleep(1)


def gen_test_data(_tracer, _logger):
    trace_id = int("0x7648f5b2583007f9d007739dde452a41", 0)
    span_id = int("0xd3b6efb24e645484", 0)
    # context=trace.SpanContext(
    #     trace_id=trace_id,
    #     span_id=span_id,
    #     is_remote=True,
    # )
    with _tracer.start_as_current_span("service1"):
        log_msg(_logger, "Starting span service1")
        span = trace.get_current_span()
        try:
            raise ValueError('Invalid input')
        except ValueError as e:
            span.record_exception(e)
        # Get trace context
        span_context = span.get_span_context()
        is_sampled = span_context.trace_flags > 0
        print(f"span > {span_info(span_context)}, sampled={is_sampled}")
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
            print(f"span > {span_info(span_context)}")
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


class RequestStats:
    def __init__(self, metrics=None, meter_name="hb_test"):
        self.stat_items = []
        self.request_count = 0

        meter = metrics.get_meter(meter_name)
        meter.create_observable_gauge(
            f"{meter_name}_process_time",
            callbacks=[self.request_time_taken_observations],
        )
        meter.create_observable_counter(
            f"{meter_name}_requests",
            callbacks=[self.request_count_observation],
        )

    def add(self, process_time, tenant="test_tenant", msg_id="msg_id_0001"):
        self.request_count = self.request_count + 1
        self.stat_items.append(RequestStatItem(process_time, tenant, msg_id))

    def request_count_observation(self, options: CallbackOptions = CallbackOptions()):
        print(f"request_count_observation = {self.request_count}")
        yield Observation(self.request_count)
        self.request_count = 0

    def request_time_taken_observations(self, options: CallbackOptions = CallbackOptions()):
        idx = 0
        while True:
            if len(self.stat_items) == 0:
                break
            item = self.stat_items.pop()
            idx = idx + 1
            print(f"time_taken_observations = {idx} {item.process_time} {item.attributes}")
            yield Observation(item.process_time, item.attributes)


class RequestStatItem:
    def __init__(self, process_time, tenant, msg_id):
        self.process_time = process_time
        self.attributes = {"tenant": tenant, "msg_id": msg_id}


class HelloHandler(helloworld_pb2_grpc.GreeterServicer):
    def __init__(self, service_name, tracer=None, logger=None, stats=None):
        self.service_name = service_name;
        self.tracer = tracer
        self.logger = logger
        self.stats = stats
        print(f"{service_name} started")

    def SayHello(self, request, context):
        span = trace.get_current_span()
        self.log_msg(span, "got request")
        print(f"span > {span_info(span.get_span_context())}")
        # gen_linked_span_context(_tracer, _logger)
        request_from = getattr(request, "name")
        self.log_msg(span, f"request from {request_from}")
        request_from = f"{request_from} -> {self.service_name}"
        start = time.time()
        time.sleep(.25)
        response = self.handle_message(request_from)
        self.stats.add(int(round(time.time() - start, 3) * 1000))
        self.log_msg(span, "got response")
        return response

    def handle_message(self, request_from):
        response = helloworld_pb2.HelloReply(message=f"Hello [{request_from}]")
        return response

    def log_msg(self, span, msg):
        msg = f"{self.service_name} - {msg}"
        print(msg)
        self.logger.error(msg)
        span.add_event(msg)

def start_grpc_server(port, handler):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    helloworld_pb2_grpc.add_GreeterServicer_to_server(handler, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server started, listening on {port}")
    server.wait_for_termination()


def grpc_otel_config(service_name):
    logging.basicConfig()
    _tracer = configure_tracing(service_name=service_name)
    _logger = configure_logging(service_name=service_name)
    _metrics = configure_metrics(service_name=service_name)
    stats = RequestStats(metrics=_metrics, meter_name=service_name)
    grpc_instrumentation.GrpcInstrumentorServer().instrument()
    grpc_instrumentation.GrpcInstrumentorClient().instrument()
    return _tracer, _logger, _metrics, stats

def span_info(span_context):
    return f"trace_id={hex(span_context.trace_id)}, span_id={hex(span_context.span_id)}"

if __name__ == "__main__":
    vm_endpoint = "http://104.155.132.143:4318"
    tracer = configure_tracing(vm_endpoint)
    logger = configure_logging(vm_endpoint)
    gen_test_data(tracer, logger)
    print("Done")
