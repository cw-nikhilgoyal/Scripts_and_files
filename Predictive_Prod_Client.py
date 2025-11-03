import grpc
import pandas as pd
from google.protobuf.message import DecodeError

# Import your protobufs
# Add latest proto files from Predictive and API gateway 
# Command EG: python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. gateway_microservice.proto

from PredictiveScore_pb2 import UsedCarDetailsV3, CarValuationResponseV3
from gateway_microservice_pb2 import InputRequest, OutputRequest
from gateway_microservice_pb2_grpc import gateway_microserviceStub

EXCEL_PATH = "Test.csv" # File name
SERVICE_HOST = "10.10.xx.yyy:50052" # Remove this IP and add IP for predictive service POD
TARGET_FUNCTION = "GetUsedCarValuationV3" # RPC name to hit

# Read CSV
df = pd.read_csv(EXCEL_PATH)
responses = []

# Setup gRPC channel and stub for the gateway
channel = grpc.insecure_channel(SERVICE_HOST)
stub = gateway_microserviceStub(channel)

for idx, row in df.iterrows():
    # Build the original predictive request
    req_msg = UsedCarDetailsV3(
        Kilometers=int(row["Kilometers"]),
        Owners=int(row["Owners"]),
        EntryYear=int(row["EntryYear"]),
        MakeYear=int(row["MakeYear"]),
        CarVersionId=int(row["CarVersionId"]),
        StateId=int(row["StateId"])
    )

    # Serialize payload to bytes
    payload_bytes = req_msg.SerializeToString()

    # Build InputRequest for the gateway
    gateway_req = InputRequest(
        functionName=TARGET_FUNCTION,
        payload=payload_bytes,
        sequence=idx  # optional tracking id
    )

    try:
        # Send to gateway
        gw_response = stub.sendReqToMicroService(gateway_req)

        # Deserialize OutputRequest.payload
        car_resp = CarValuationResponseV3()
        car_resp.ParseFromString(gw_response.payload)

        responses.append(str(car_resp))
    except DecodeError:
        responses.append("Error: Could not decode response payload")
    except Exception as e:
        responses.append(f"Error: {e}")

# Save responses back to CSV
df["ValuationResponseV3"] = responses # Change the column name as required
df.to_csv(EXCEL_PATH, index=False)
print("✅ Done! Responses written to", EXCEL_PATH)
