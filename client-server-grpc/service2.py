import threading
import time

import helloworld_pb2
import tracing_lib
from opentelemetry import trace


class Service2(tracing_lib.HelloHandler):
    def __init__(self, service_name, tracer=None, logger=None, stats=None):
        super().__init__(service_name, tracer, logger, stats)

    def handle_message(self, request_from):
        span = trace.get_current_span()
        try:
            raise ValueError('Invalid input')
        except ValueError as e:
            span.record_exception(e)
        response = helloworld_pb2.HelloReply(message=f"Hello [{request_from}]")
        time.sleep(.3)
        linked_span_context = self.gen_linked_span_context()
        with self.tracer.start_as_current_span("child_linked_span",links=[trace.Link(context=linked_span_context)]) as linked_span:
            self.log_msg(linked_span, "Started span service_1")
            time.sleep(.3)
        return response

    def gen_linked_span_context(self):
        with self.tracer.start_as_current_span("linked-span",context=trace.Context()) as linked_span:
            self.log_msg(linked_span, "Starting span linked-service")
            linked_span.set_attribute("attr1", "linked-attr1")
            linked_span.set_attribute("attr2", "linked-attr2")
            linked_span.add_event("event-1")
            print(f"linked_span > {tracing_lib.span_info(linked_span.get_span_context())}")
            with self.tracer.start_as_current_span("child-span") as child_span:
                self.log_msg(child_span, "Starting child-span")
                child_span.set_attribute("attr1", "child-attr1")
                child_span.set_attribute("attr2", "child-attr2")
                child_span.add_event("event-2")
                print(f"child_span > {tracing_lib.span_info(child_span.get_span_context())}")
                threading.Thread(target=self.linked_child_span, args=(child_span,)).start()
            return linked_span.get_span_context()

    def linked_child_span(self, parent_span):
        parent_span_context = trace.set_span_in_context(parent_span)
        with self.tracer.start_as_current_span("child-sub-span", context=parent_span_context) as span:
            span.set_attribute("attr1", "child-sub-attr1")
            span.set_attribute("attr2", "child-sub-attr2")
            span.add_event("event-3")
            time.sleep(2)
            print(f"linked_span_done > {tracing_lib.span_info(span.get_span_context())}")

if __name__ == "__main__":
    service_name = "otel_test_service_2"
    tracer, logger, metrics, stats = tracing_lib.grpc_otel_config(service_name=service_name)
    handler = Service2(service_name="service2", tracer=tracer, logger=logger, stats=stats)
    tracing_lib.start_grpc_server(50052, handler)
