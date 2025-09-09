from kafka import KafkaConsumer
from Transaction import Transaction   # ✅ Import the Pydantic model
import json

print("Consumer with Pydantic started...")

# Kafka Consumer Setup
consumer = KafkaConsumer(
    "dataset_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",   # read from beginning if no committed offset
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Read and Validate
for message in consumer:
    record = message.value
    try:
        customer = Transaction(**record)   # ✅ Pydantic validation
        print("✅ Valid:", customer.dict())   # dict() gives clean Python dict
    except Exception as e:
        print("❌ Invalid:", record)
        print("Reason:", e)
