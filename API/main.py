from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from uuid import uuid4
import json

app = FastAPI()

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SUBSCRIPTIONS_TOPIC = "subscriptions"

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Subscription models
class SubscriptionCondition(BaseModel):
    field: str
    operator: str
    value: str

class Subscription(BaseModel):
    conditions: list[SubscriptionCondition]

@app.post("/subscribe")
def create_subscription_topic(subscription: Subscription):
    topic_id = f"subscription-{uuid4().hex[:8]}"

    try:
        # Create the dedicated topic
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        topic = NewTopic(name=topic_id, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)

        # Push the subscription to the "subscriptions" topic
        payload = {
            "topic_id": topic_id,
            "subscription": subscription.dict()
        }
        producer.send(SUBSCRIPTIONS_TOPIC, value=payload)
        producer.flush()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka failure: {str(e)}")

    return {"topic_id": topic_id}
