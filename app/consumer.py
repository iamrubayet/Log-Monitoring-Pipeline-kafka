from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values
import json

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="logsdb",
    user="user",
    password="password",
    host="postgres",
    port="5432"
)
cursor = conn.cursor()

# Kafka consumer setup
consumer = KafkaConsumer(
    'logs_topic',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='log_db_inserter_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def bulk_insert_logs(logs):
    insert_query = "INSERT INTO logs (log_message) VALUES %s"
    execute_values(cursor, insert_query, [(log['log'],) for log in logs])
    conn.commit()
    print(f"Inserted {len(logs)} logs into the database")

batch_size = 100  # Insert in batches of 100 logs
logs_batch = []

for message in consumer:
    log_message = message.value
    logs_batch.append(log_message)

    if len(logs_batch) >= batch_size:
        bulk_insert_logs(logs_batch)
        logs_batch.clear()

# Insert any remaining logs
if logs_batch:
    bulk_insert_logs(logs_batch)

cursor.close()
conn.close()
