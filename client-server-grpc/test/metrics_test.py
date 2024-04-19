from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.export import PeriodicExportingMetricReader

#from opentelemetry.sdk.metrics.export import MetricsExporter, MetricsExportResult
#from opentelemetry.sdk.metrics.export.controller import PushController

# Set up the OTLP exporter
#exporter = OTLPMetricExporter(endpoint="http://34.28.153.88:443")
exporter = OTLPMetricExporter(endpoint="http://34.72.18.251:4317", insecure=True)
reader = PeriodicExportingMetricReader(exporter)
provider = MeterProvider(metric_readers=[reader])
metrics.set_meter_provider(provider)

# Set up the meter provider
meter = metrics.get_meter("otel_test3")

# Set up the push controller
#controller = PushController(meter=meter, exporter=exporter)
#controller.start()

# Create a counter instrument
counter = meter.create_counter(
    name="requests",
    description="number of requests",
    unit="1",
)

# Record some data
counter.add(1)

#push
exporter.force_flush()

exporter.shutdown()

# Wait for the push controller to export the data
#controller.shutdown()