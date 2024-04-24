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
"""The Python implementation of the GRPC helloworld.Greeter client."""

from __future__ import print_function

import logging

import grpc
import helloworld_pb2
import helloworld_pb2_grpc
from opentelemetry.instrumentation import grpc as grpc_instrumentation

import tracing_lib
import time



def run(stats, tenant, msg_id):
    print("Will try to greet world ...")
    start = time.time()
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = helloworld_pb2_grpc.GreeterStub(channel)
        #response = stub.SayHello(helloworld_pb2.HelloRequest(name=f"hello?tenant={tenant}&msg_id={msg_id}"))
        response = stub.SayHello(helloworld_pb2.HelloRequest(name="client"))
        time.sleep(0.1)
    stats.add(process_time=int(round(time.time() - start,3)*1000), tenant=tenant, msg_id=msg_id)
    print("Greeter client received: " + response.message)

if __name__ == "__main__":
    service_name = "otel_test_client"
    tracer, logger, metrics, stats = tracing_lib.grpc_otel_config(service_name=service_name)
    for msg_idx in range(1):
        for tenant in ["tenant1"]:
            run(stats, tenant, str(msg_idx))
            time.sleep(.1)
        time.sleep(.3)
