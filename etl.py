from minio import Minio
from kafka import KafkaConsumer
import json
import io

client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False,
)

kafka_consumer = KafkaConsumer(
    "ingest",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest"
)

def check_bucket_exists(bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists")
          
def create_object_in_minio(json_data, bucket_name, object_name):
    data = json.dumps(json_data).encode("utf-8")
    client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )

if __name__ == "__main__":
    bucket_name = "data-lake"
    check_bucket_exists(bucket_name)
    
    # Works like a While True
    for message in kafka_consumer:
        #
        # Place your transformation function here
        #
        data = json.loads(message.value.decode("utf-8"))
        file_name = f"{message.key.decode('utf-8')}.json"
        create_object_in_minio(data, bucket_name, file_name)
        print(f"Data {data} written to Minio as {file_name}")