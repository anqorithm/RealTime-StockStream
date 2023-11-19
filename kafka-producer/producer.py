import json
import random
import uuid
import socket
from datetime import datetime
from kafka import KafkaProducer
import time

stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "FB", "TSLA", "BRK.A", "V", "JPM",
          "JNJ", "UNH", "WMT", "PG", "MA", "DIS", "NVDA", "HD", "PYPL", "BAC", "CMCSA"]


def is_kafka_running(server):
    try:
        host, port = server.split(":")
        socket.create_connection((host, int(port)), timeout=10)
        print("Kafka server is running.")
        return True
    except socket.error:
        print("Kafka server is not running.")
        return False


def create_producer():
    return KafkaProducer(
        bootstrap_servers='172.28.1.2:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def send_data(producer, topic, data):
    try:
        producer.send(topic, value=data).get(timeout=60)
        print(f"Data sent: {data}")
    except Exception as e:
        print(f"Error sending data: {e}")


def main():
    server = '172.28.1.2:9092'
    if not is_kafka_running(server):
        print("Error: Kafka server is not running.")
        return

    producer = create_producer()
    topic = 'stocks'

    while True:
        data = {
            "stock": random.choice(stocks),
            "trade_id": str(uuid.uuid4()),
            "price": round(random.uniform(100, 200), 2),
            "quantity": random.randint(1, 100),
            "trade_type": random.choice(["buy", "sell"]),
            "trade_date": datetime.now().strftime("%Y-%m-%d"),
            "trade_time": datetime.now().strftime("%H:%M:%S")
        }

        send_data(producer, topic, data)
        time.sleep(1)


if __name__ == "__main__":
    main()
