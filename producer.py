from kafka import KafkaProducer
import uuid
from random import randint, choice
import json
import time


cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
names = ["John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles", "Daniel"]

if "__main__" == __name__:
    kafka_producer = KafkaProducer(
      bootstrap_servers="localhost:9092"
    ) 

    while True:
        # A Dictionary object simulates json data
        data = {
          "name": choice(names),
          "age": randint(20, 60),
          "city": choice(cities),
        }
        key = str(uuid.uuid4())
        kafka_producer.send(
            topic="ingest",
            value=json.dumps(data).encode("utf-8"),
            key=key.encode("utf-8"),
        )
        print(f"Data {data} sent to Kafka with key {key}")
        time.sleep(1)
        
