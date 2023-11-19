import sys
import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


def on_send_success(record_metadata):
    print("Message sent to topic:", record_metadata.topic)
    print("Partition:", record_metadata.partition)
    print("Offset:", record_metadata.offset)


def on_send_error(excp):
    print('Error:', excp)


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,  # Retry configuration
            retry_backoff_ms=2000  # Wait time between retries
        )
        return producer
    except KafkaError as e:
        print(f"Failed to create Kafka producer: {e}")
        sys.exit(1)


def produce_data(producer):
    while True:
        data = {
            "stock": "AAPL",
            "trade_id": str(uuid.uuid4()),
            "price": round(random.uniform(100, 200), 2),
            "quantity": random.randint(1, 100),
            "trade_type": random.choice(["buy", "sell"]),
            "trade_date": datetime.now().strftime("%Y-%m-%d"),
            "trade_time": datetime.now().strftime("%H:%M:%S")
        }
        print("Sending data:", data)
        future = producer.send('stocks', value=data)
        future.add_callback(on_send_success).add_errback(on_send_error)

        try:
            producer.flush()
        except KafkaTimeoutError as e:
            print(f"Timeout error while sending data: {e}")
        except KafkaError as e:
            print(f"Failed to send data: {e}")
        time.sleep(1)  # Adjust sleep time as needed


if __name__ == "__main__":
    try:
        kafka_producer = create_producer()
        produce_data(kafka_producer)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
