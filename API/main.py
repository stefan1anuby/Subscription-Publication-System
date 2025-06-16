from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from uuid import uuid4

app = FastAPI()

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SUBSCRIPTIONS_TOPIC = "subscriptions"

# Kafka producer (now sending plain string)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode("utf-8")  # ✅ send string, not JSON
)

# Subscription models
class SubscriptionCondition(BaseModel):
    field: str
    operator: str
    value: str

class Subscription(BaseModel):
    conditions: list[SubscriptionCondition]

def format_subscription_to_string(subscription: Subscription) -> str:
    """
    Convert a Subscription object to your DSL-style string, like:
    {(city,=,"Bucharest");(temp,>=,10);(wind,<,11)}
    """
    condition_strs = []
    for cond in subscription.conditions:
        # Quote value if it's not numeric
        val = f'"{cond.value}"' if not cond.value.isnumeric() else cond.value
        condition_strs.append(f"({cond.field},{cond.operator},{val})")
    return "{" + ";".join(condition_strs) + "}"

@app.post("/subscribe")
def create_subscription_topic(subscription: Subscription):
    topic_id = f"subscription-{uuid4().hex[:8]}"

    try:
        # 1. Create a new Kafka topic for this subscription
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        topic = NewTopic(name=topic_id, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)

        # 2. Convert the subscription to DSL-string format
        sub_str = format_subscription_to_string(subscription)

        # 3. Send to central "subscriptions" topic
        payload = f"{topic_id}##{sub_str}"
        producer.send(SUBSCRIPTIONS_TOPIC, value=payload)
        producer.flush()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka failure: {str(e)}")

    return {"topic_id": topic_id}

# uvicorn main:app --reload