from kafka import KafkaProducer
import uuid
from random import randint, choice


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
        kafka_producer.send(
            topic="ingest",
            value=data.encode("utf-8"),
            key=uuid.encode("utf-8")
        )
