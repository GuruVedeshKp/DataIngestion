from kafka import KafkaProducer
import pandas as pd
import json

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load dataset
df = pd.read_csv("CustomersData.csv")

# Send rows to Kafka
for _, row in df.iterrows():
    producer.send("dataset_topic", value=row.to_dict())

producer.flush()
print("✅ Data sent to Kafka")
