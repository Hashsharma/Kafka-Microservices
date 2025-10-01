import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel

# --- Configuration ---
KAFKA_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = "fastapi_messages"

# Global producer instance
producer = None

# --- Pydantic Schema ---
class Message(BaseModel):
    key: str
    message: str

# --- Lifecycle Management for KafkaProducer ---
# Use the lifespan context manager to manage resources (like the Kafka Producer)
# that should be initialized before the app starts and cleaned up when it shuts down.
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    print(f"Connecting to Kafka at: {KAFKA_SERVER}")
    try:
        # Initialize the Kafka Producer
        # value_serializer converts Python objects (like dictionaries) to JSON bytes
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 2),
            # Set a low timeout to quickly fail if the broker is unreachable 
            api_version_auto_timeout_ms=5000 
        )
        
        # --- NOTE: Removed the line 'producer.partitions_for_topic(KAFKA_TOPIC)' ---
        # We rely on the Docker healthcheck (in docker-compose.yml) 
        # to ensure Kafka is ready and on the future.get() in the /produce endpoint
        # to handle transient connection issues.
        
        print("Kafka Producer initialized.")
    except Exception as e:
        print(f"Error connecting to Kafka (initialization failed): {e}")
        # If construction fails, producer remains None
        producer = None 

    yield
    
    # --- Shutdown ---
    if producer:
        print("Closing Kafka Producer...")
        producer.close()
        print("Kafka Producer closed.")

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Kafka Producer Service", 
    version="1.0",
    lifespan=lifespan
)

# --- Routes ---

@app.get("/")
def read_root():
    """Root endpoint for status check."""
    return {"service": "Kafka Producer", "topic": KAFKA_TOPIC, "kafka_server": KAFKA_SERVER}

@app.post("/produce")
async def produce_message(msg: Message):
    """
    Publishes a message to the configured Kafka topic.
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka Producer is not initialized. Check service logs for connection errors.")

    try:
        data = {"content": msg.message}
        
        # Send the message. The key is used for partitioning.
        future = producer.send(
            topic=KAFKA_TOPIC,
            key=msg.key.encode('utf-8'),
            value=data
        )

        # Wait for the message to be sent and log metadata (This is where true connectivity is tested)
        record_metadata = future.get(timeout=10)
        
        print(f"Message sent successfully!")
        print(f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

        return {
            "status": "success",
            "message": "Message published to Kafka",
            "key": msg.key,
            "data": msg.message
        }

    except Exception as e:
        # Catch exceptions like KafkaTimeoutError from future.get()
        producer.flush() 
        print(f"Error sending message: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish message: {e}. Kafka broker may be down or unreachable.")
