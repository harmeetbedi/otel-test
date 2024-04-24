import grpc
import helloworld_pb2
import helloworld_pb2_grpc
import tracing_lib
from opentelemetry import trace


class Service1(tracing_lib.HelloHandler):
    def __init__(self, service_name, tracer=None, logger=None, stats=None):
        super().__init__(service_name, tracer, logger, stats)

    def handle_message(self, request_from):
        print("handle_message")
        with grpc.insecure_channel("localhost:50052") as channel:
            span = trace.get_current_span()
            self.log_msg(span, "server_1 - sending to server_2")
            stub = helloworld_pb2_grpc.GreeterStub(channel)
            response = stub.SayHello(helloworld_pb2.HelloRequest(name=request_from), timeout=30)
            self.log_msg(span, f"server_1 - got response from server2 {response}")
            value = getattr(response, "message")
            value = f"{value} - resp server_1"
            # linked_span_context = self.gen_linked_span_context()
            # with self.tracer.start_as_current_span("span_with_link", links=[trace.Link(context=linked_span_context)]) as another_span:
            #     self.log_msg(another_span, "Started span with links")
            return helloworld_pb2.HelloReply(message=value)


if __name__ == "__main__":
    service_name = "otel_test_service_1"
    tracer, logger, metrics, stats = tracing_lib.grpc_otel_config(service_name=service_name)
    handler = Service1(service_name="service1", tracer=tracer, logger=logger, stats=stats)
    tracing_lib.start_grpc_server(50051, handler)
