# Kafka Demo
This project provides a simple Kafka environment and Python scripts to create, delete, produce, and consume messages.

# Setup
1. Install dependencies
```shel
pip install -r requirements.txt
```
2. Use docker-compose.yml to run a local Kafka service via Docker:
```shell
docker-compose up -d
```
# Usage
1. Create or delete topics with create_topic or delete_topic:
```shell
python admin.py create -n test --partition 1 --replication 1
python admin.py delete -n test
```
2. Produce messages with producer.py:
```shell
python producer.py
```
3. Consume messages with consumer.py:
```shell
python consumer.py
```
