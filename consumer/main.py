import os
import json
import asyncio
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# --- Config ---
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REQUEST_TOPIC = "fastapi_messages"
REPLY_TOPIC = "fastapi_replies"
CONSUMER_GROUP = "fastapi_consumer_group"

# --- Globals ---
consumer: Optional[AIOKafkaConsumer] = None
producer: Optional[AIOKafkaProducer] = None

# --- FastAPI ---
app = FastAPI(title="Kafka Consumer with reply", version="1.0")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer, producer

    # Initialize producer (to send replies)
    producer = AIOKafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    print("Kafka Producer for replies started.")

    # Initialize consumer (to receive requests)
    consumer = AIOKafkaConsumer(
        REQUEST_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print("Kafka Consumer started.")

    # Start background task for consuming
    app.state.consumer_task = asyncio.create_task(consume_loop())

    yield

    # Shutdown
    app.state.consumer_task.cancel()
    try:
        await app.state.consumer_task
    except asyncio.CancelledError:
        pass

    await consumer.stop()
    await producer.stop()
    print("Kafka Consumer and Producer stopped.")

app.router.lifespan_context = lifespan

async def consume_loop():
    async for msg in consumer:
        key = msg.key.decode("utf-8") if msg.key else None
        value = msg.value
        content = value.get("content", "")
        length = len(content)

        print(f"Received message key={key} content='{content}', length={length}")

        # Prepare reply message
        reply_data = {"length": length,
                      "some_value": "It is coming from consumer"}

        if key:
            await producer.send_and_wait(
                topic=REPLY_TOPIC,
                key=key.encode("utf-8"),
                value=reply_data,
            )
            print(f"Sent reply for key={key}: {reply_data}")
