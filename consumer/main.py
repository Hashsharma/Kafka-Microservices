import os
import json
import asyncio
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
from kafka import KafkaConsumer, TopicPartition

# --- Configuration ---
KAFKA_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = "fastapi_messages"
CONSUMER_GROUP = "fastapi_group_1"

# --- Global State ---
# This list will store all messages consumed from Kafka
MESSAGES_RECEIVED = []
consumer_thread = None
stop_event = threading.Event()

# --- Kafka Consumer Logic ---

def kafka_listener(stop_event: threading.Event):
    """
    Runs the Kafka Consumer loop in a separate thread.
    """
    print(f"Attempting to start Kafka Consumer for topic: {KAFKA_TOPIC}")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            group_id=CONSUMER_GROUP,
            # Deserialize the message value from JSON bytes back to a Python object
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # Start reading from the earliest available message if no offset is saved
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            api_version=(0, 10, 2)
        )
        print("Kafka Consumer started successfully.")
    except Exception as e:
        print(f"Error initializing Kafka Consumer: {e}")
        return

    # Main consumption loop
    try:
        for message in consumer:
            if stop_event.is_set():
                break # Exit the loop if the application is shutting down
            
            print(f"Consumed message: Topic={message.topic}, Key={message.key.decode('utf-8')}, Value={message.value}")
            
            # Append the consumed message data to the global list
            MESSAGES_RECEIVED.append({
                "key": message.key.decode('utf-8'),
                "value": message.value.get("content"),
                "timestamp": message.timestamp
            })
            
    except Exception as e:
        print(f"Error during Kafka consumption: {e}")
    finally:
        print("Closing Kafka Consumer.")
        consumer.close()

# --- Lifecycle Management ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_thread
    
    # --- Startup ---
    print("Starting Kafka Consumer Thread...")
    stop_event.clear()
    consumer_thread = threading.Thread(target=kafka_listener, args=(stop_event,))
    consumer_thread.daemon = True # Allows the main process to exit even if the thread is running
    consumer_thread.start()
    
    yield
    
    # --- Shutdown ---
    print("Stopping Kafka Consumer Thread...")
    stop_event.set() # Signal the consumer thread to stop
    if consumer_thread and consumer_thread.is_alive():
        # Wait for the thread to finish gracefully (with a timeout)
        consumer_thread.join(timeout=5)
        print("Kafka Consumer Thread stopped.")


# --- FastAPI App Initialization ---
app = FastAPI(
    title="Kafka Consumer Service", 
    version="1.0",
    lifespan=lifespan
)

# --- Routes ---

@app.get("/")
def read_root():
    """Root endpoint for status check."""
    return {
        "service": "Kafka Consumer", 
        "topic": KAFKA_TOPIC, 
        "kafka_server": KAFKA_SERVER,
        "status": "Running",
        "consumer_thread_alive": consumer_thread.is_alive() if consumer_thread else False
    }

@app.get("/messages")
async def get_messages():
    """
    Returns all messages consumed and stored in memory.
    """
    return {"count": len(MESSAGES_RECEIVED), "messages": MESSAGES_RECEIVED}
