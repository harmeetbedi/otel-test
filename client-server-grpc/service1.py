# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC helloworld.Greeter server."""
import time
from concurrent import futures
import logging

import grpc
import helloworld_pb2
import helloworld_pb2_grpc
import tracing_lib
from opentelemetry.instrumentation import grpc as grpc_instrumentation
from opentelemetry import trace


class Greeter(helloworld_pb2_grpc.GreeterServicer):

  def SayHello(self, request, context):
    span = trace.get_current_span()
    log_msg(span,"server1 - got hello request")
    print(f"span > {span}")
    request_from = getattr(request, "name")
    log_msg(span, f"server1 - request from {request_from}")
    request_from = f"{request_from} -> server1"
    return self.send_msg(request_from)

  def send_msg(self, request_from):
    with grpc.insecure_channel("localhost:50052") as channel:
      span = trace.get_current_span()
      log_msg(span,"server_1 - sending to server_2")
      stub = helloworld_pb2_grpc.GreeterStub(channel)
      response = stub.SayHello(helloworld_pb2.HelloRequest(name=request_from), timeout=2)
      log_msg(span, f"server_1 - got response from server2 {response}")
      value = getattr(response, "message")
      value = f"{value} - resp server_1"
      return helloworld_pb2.HelloReply(message=value)


def log_msg(span, msg):
  print(msg)
  logger.error(msg)
  #time.sleep(.1)
  span.add_event(msg)

def gen_linked_span_context(_tracer, _logger):
  with _tracer.start_as_current_span("linked-service") as span:
    log_msg(_logger, "Starting span linked-service")
    span_context = span.get_span_context()
    print(f"linked_span > trace_id={hex(span_context.trace_id)}, span_id={hex(span_context.span_id)}")
    return span.get_span_context()


def serve():
  port = "50051"
  grpc_instrumentation.GrpcInstrumentorServer().instrument()
  grpc_instrumentation.GrpcInstrumentorClient().instrument()
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
  server.add_insecure_port("[::]:" + port)
  server.start()
  print("Server started, listening on " + port)
  server.wait_for_termination()


if __name__ == "__main__":
  logging.basicConfig()
  vm_endpoint = "http://34.72.18.251:4318"
  tracer = tracing_lib.configure_tracing(vm_endpoint, service_name="service_1")
  logger = tracing_lib.configure_logging(vm_endpoint)
  #linked_span_context = gen_linked_span_context(tracer, logger)
  serve()
