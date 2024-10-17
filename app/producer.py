from kafka import KafkaProducer
import time
import random
import json

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

logs = [
    "INFO: User 123 logged in",
    "ERROR: Server overload",
    "WARN: Disk space low",
    "INFO: Data processed successfully",
    "ERROR: Database connection failed"
]

def generate_log():
    log = random.choice(logs)
    print(f"Produced log: {log}")
    return log

while True:
    log_message = generate_log()
    # Key helps with partitioning, e.g., by log type (INFO, ERROR, etc.)
    key = log_message.split(":")[0].encode('utf-8')
    producer.send('logs_topic', key=key, value={"log": log_message})
    time.sleep(2)
