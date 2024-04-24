import grpc
import helloworld_pb2
import helloworld_pb2_grpc

import tracing_lib
import time

def run(stats, tenant, msg_id):
    print(f"Sending Client Request {tenant} {msg_id}")
    start = time.time()
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = helloworld_pb2_grpc.GreeterStub(channel)
        data = {"src":"client", "tenant":tenant,"msg_id":msg_id}
        response = stub.SayHello(helloworld_pb2.HelloRequest(name=str(data)))
        time.sleep(0.5)
    stats.add(process_time=int(round(time.time() - start,3)*1000), tenant=tenant, msg_id=msg_id)
    print("Greeter client received: " + response.message)

if __name__ == "__main__":
    service_name = "otel_test_client"
    tracer, logger, metrics, stats = tracing_lib.grpc_otel_config(service_name=service_name)
    for msg_idx in range(200):
        for tenant in ["tenant1","tenant2"]:
            run(stats, tenant, str(msg_idx+1))
        time.sleep(2)
