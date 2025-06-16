import requests
import json
import sys
from kafka import KafkaConsumer

API_URL = "http://localhost:8000/subscribe"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Example subscription payloads
subscriptions = [
    {
        "conditions": [
            {"field": "city", "operator": "=", "value": "Iasi"},
            {"field": "temp", "operator": ">=", "value": "10"}
        ]
    },
    {
        "conditions": [
            {"field": "city", "operator": "=", "value": "Cluj"},
            {"field": "wind", "operator": "<", "value": "15"}
        ]
    },
    {
        "conditions": [
            {"field": "temp", "operator": ">", "value": "25"},
            {"field": "wind", "operator": "<=", "value": "20"}
        ]
    }
]

def main(index):
    if index < 0 or index >= len(subscriptions):
        print(f"[ERROR] Invalid subscription index. Choose 0 to {len(subscriptions) - 1}")
        return

    subscription_payload = subscriptions[index]

    print(f"[INFO] Using subscription: {json.dumps(subscription_payload)}")

    # Send request to subscribe
    response = requests.post(API_URL, json=subscription_payload)
    if response.status_code != 200:
        print(f"[ERROR] Failed to subscribe: {response.text}")
        return

    topic_id = response.json().get("topic_id")
    print(f"[INFO] Subscribed successfully. Listening to topic: {topic_id}")

    # Consume messages from Kafka
    consumer = KafkaConsumer(
        topic_id,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"client-{topic_id}",
        value_deserializer=lambda v: v.decode("utf-8")
    )

    for message in consumer:
        print(f"[{topic_id}] Received: {message.value}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <subscription_index>")
        sys.exit(1)

    try:
        index = int(sys.argv[1])
        main(index)
    except ValueError:
        print("[ERROR] Subscription index must be an integer.")
        sys.exit(1)


# python client.py 0
# python client.py 1
# python client.py 2