import os
import json
import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import Optional, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# --- Config ---
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REQUEST_TOPIC = "fastapi_messages"
REPLY_TOPIC = "fastapi_replies"

# --- Globals ---
producer: Optional[AIOKafkaProducer] = None
reply_consumer: Optional[AIOKafkaConsumer] = None
pending_replies: Dict[str, asyncio.Future] = {}

# --- Pydantic ---
class Message(BaseModel):
    key: Optional[str]
    message: str

# --- FastAPI ---
app = FastAPI(title="Kafka Producer with Reply", version="1.0")

# --- Kafka lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, reply_consumer
    
    # Producer setup
    producer = AIOKafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    print("Kafka Producer started.")
    
    # Reply consumer setup
    reply_consumer = AIOKafkaConsumer(
        REPLY_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        group_id="fastapi_producer_reply_group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await reply_consumer.start()
    print("Kafka Reply Consumer started.")

    # Start background task to listen for replies
    app.state.reply_task = asyncio.create_task(reply_listener())
    
    yield
    
    # Shutdown
    app.state.reply_task.cancel()
    try:
        await app.state.reply_task
    except asyncio.CancelledError:
        pass
    
    await reply_consumer.stop()
    await producer.stop()
    print("Kafka Producer and Reply Consumer stopped.")

app.router.lifespan_context = lifespan

# --- Reply listener ---

async def reply_listener():
    async for msg in reply_consumer:
        key = msg.key.decode("utf-8") if msg.key else None
        data = msg.value
        if key and key in pending_replies:
            future = pending_replies.pop(key)
            future.set_result(data)
            print(f"Reply received for key={key}: {data}")

# --- Routes ---

@app.get("/")
def root():
    return {"service": "Kafka Producer with reply", "request_topic": REQUEST_TOPIC, "reply_topic": REPLY_TOPIC}

@app.post("/produce")
async def produce_message(msg: Message):
    if not producer:
        raise HTTPException(503, "Kafka Producer is not initialized")
    
    # Use given key or generate UUID
    correlation_id = msg.key or str(uuid.uuid4())
    data = {"content": msg.message}
    
    future = asyncio.get_event_loop().create_future()
    pending_replies[correlation_id] = future

    try:
        await producer.send_and_wait(
            topic=REQUEST_TOPIC,
            key=correlation_id.encode("utf-8"),
            value=data,
        )
        print(f"Message sent with key={correlation_id}")

        # Wait for reply (timeout 10 seconds)
        reply = await asyncio.wait_for(future, timeout=10)
        return {"status": "success", "key": correlation_id, "reply": reply}

    except asyncio.TimeoutError:
        pending_replies.pop(correlation_id, None)
        raise HTTPException(504, "Timeout waiting for reply")

    except Exception as e:
        pending_replies.pop(correlation_id, None)
        raise HTTPException(500, f"Error producing message: {e}")
