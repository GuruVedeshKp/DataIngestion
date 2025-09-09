from kafka import KafkaConsumer
import json

print("Consumer started...")

# Kafka Consumer Setup
consumer = KafkaConsumer(
    "dataset_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",   # 👈 read from beginning if no committed offset
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Validation Function
def validate(record):
    return True

# Read and Validate
for message in consumer:
    record = message.value
    if validate(record):
        print("✅ Valid:", record)
        
    else:
        print("❌ Invalid:", record)
