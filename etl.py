from minio import Minio
import json
import io

client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False,
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
    data = {
        "name": "John",
        "age": 30,
        "city": "New York",
    }
    bucket_name = "data-lake"
    check_bucket_exists(bucket_name)
    create_object_in_minio(data, bucket_name, "user.json")
    print("Object created successfully")