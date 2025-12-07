import json
import uuid
import os
import threading
import asyncio
import queue
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from confluent_kafka import Producer, Consumer
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Load environment variables from .env file FIRST
load_dotenv()

app = FastAPI()

# Allow local dev frontends (Vite default ports)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka Producer Configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

# Kafka Consumer Configuration (for real-time streaming to WebSocket clients)
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'frontend-order-stream',
    'auto.offset.reset': 'latest',  # Only new orders for live stream
}

# Track active WebSocket connections
active_connections = []

# Queue for passing orders from Kafka consumer thread to async broadcast
order_queue = queue.Queue()

# Postgres Configuration (from Neon)
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set. Please configure .env file.")

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(DATABASE_URL)


# Request model with validation
class Order(BaseModel):
    user: str = Field(..., min_length=1, description="User identifier")
    product: str = Field(..., min_length=1, description="Product name")
    quantity: int = Field(..., gt=0, description="Quantity must be positive")


def delivery_report(err, msg, order_id):
    """Callback for Kafka delivery reports"""
    if err:
        print(f"Message delivery failed for order {order_id}: {err}")
        update_order_status(order_id, 'kafka_failed', {'error': str(err)})
        # Broadcast status update
        broadcast_order_to_clients({
            'type': 'status_update',
            'order_id': order_id,
            'status': 'kafka_failed',
            'error': str(err)
        })
    else:
        metadata = {
            'kafka_topic': msg.topic(),
            'kafka_partition': msg.partition(),
            'kafka_offset': msg.offset(),
            'timestamp': datetime.utcnow().isoformat()
        }
        print(f"Message delivered for order {order_id}: {metadata}")
        update_order_status(order_id, 'kafka_published', metadata)
        # Broadcast status update to WebSocket clients
        broadcast_order_to_clients({
            'type': 'status_update',
            'order_id': order_id,
            'status': 'kafka_published',
            'kafka_metadata': metadata
        })


def update_order_status(order_id: str, status: str, metadata: dict = None):
    """Update order status and metadata in database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        metadata_json = json.dumps(metadata) if metadata else None
        
        cur.execute(
            """
            UPDATE orders 
            SET status = %s, kafka_metadata = %s, updated_at = %s
            WHERE order_id = %s
            """,
            (status, metadata_json, datetime.utcnow(), order_id)
        )
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error updating order status: {e}")


def broadcast_order_to_clients(order_message: dict):
    """Put order in queue for async broadcast (thread-safe)"""
    print(f"📦 Order queued for broadcast: {order_message}")
    order_queue.put(order_message)


def kafka_consumer_thread():
    """
    Background thread that consumes from Kafka 'orders' topic
    and broadcasts to connected WebSocket clients.
    Similar to tracker.py but sends to browsers instead of stdout.
    """
    consumer = Consumer(consumer_config)
    consumer.subscribe(['orders'])
    
    print("📡 Kafka consumer thread started. Listening for new orders...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            # Decode and parse order
            try:
                value = msg.value().decode('utf-8')
                order = json.loads(value)
                
                # Prepare message for WebSocket broadcast
                order_message = {
                    'type': 'order',
                    'order': order,
                    'kafka_offset': msg.offset(),
                    'kafka_partition': msg.partition(),
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                print(f"✅ Received order: {order['quantity']} unit(s) of {order['product']} by {order['user']}")
                
                # Broadcast to queue (thread-safe)
                broadcast_order_to_clients(order_message)
                        
            except Exception as e:
                print(f"Error processing order: {e}")
    
    except KeyboardInterrupt:
        print("Consumer interrupted")
    finally:
        consumer.close()


# Start Kafka consumer thread on app startup
@app.on_event("startup")
async def startup_event():
    """Start background Kafka consumer thread"""
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    print("🚀 Kafka consumer background thread started")


async def broadcast_queue_processor():
    """
    Continuously process orders from queue and broadcast to WebSocket clients.
    Runs as an async task in the event loop.
    """
    print("🎬 Broadcast processor started")
    loop = asyncio.get_event_loop()
    while True:
        try:
            # Non-blocking check for new orders
            order_message = order_queue.get_nowait()
            print(f"📢 Broadcasting to {len(active_connections)} clients: {order_message}")
            
            # Broadcast to all connected WebSocket clients
            for connection in active_connections:
                try:
                    await connection.send_json(order_message)
                    print(f"✅ Sent to client: {connection.client}")
                except Exception as e:
                    print(f"❌ Failed to send to client: {e}")
                    try:
                        active_connections.remove(connection)
                    except ValueError:
                        pass
        except queue.Empty:
            # Queue is empty, wait a bit before checking again
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Error in broadcast processor: {e}")
            await asyncio.sleep(1)


@app.post("/orders")
async def create_order(order: Order):
    """
    Accept an order, persist to Postgres (idempotent), then produce to Kafka.
    
    Flow:
    1. Generate order_id
    2. Insert/upsert order into Postgres (ensures idempotency)
    3. Publish to Kafka
    4. Update order status with Kafka delivery metadata
    
    Args:
        order: Order object with user, product, and quantity
        
    Returns:
        Order details with assigned order_id and DB status
    """
    order_id = str(uuid.uuid4())
    
    try:
        # Step 1: Create order payload
        order_data = {
            'order_id': order_id,
            'user': order.user,
            'product': order.product,
            'quantity': order.quantity
        }
        
        # Step 2: Upsert into Postgres (idempotent insert)
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            INSERT INTO orders (order_id, status, payload, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE
            SET status = EXCLUDED.status,
                payload = EXCLUDED.payload,
                updated_at = EXCLUDED.updated_at
            """,
            (
                order_id,
                'pending',
                json.dumps(order_data),
                datetime.utcnow(),
                datetime.utcnow()
            )
        )
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Step 3: Produce to Kafka
        json_str = json.dumps(order_data).encode('utf-8')
        
        # Use lambda to capture order_id for callback
        producer.produce(
            topic='orders',
            value=json_str,
            callback=lambda err, msg: delivery_report(err, msg, order_id)
        )
        
        # Flush to ensure message is sent
        producer.flush()
        
        return {
            "status": "success",
            "message": "Order persisted to DB and produced to Kafka",
            "order": order_data,
            "order_id": order_id
        }
    
    except psycopg2.IntegrityError as e:
        # Order already exists - idempotent behavior
        conn.rollback()
        cur.close()
        conn.close()
        return {
            "status": "success",
            "message": "Order already exists (idempotent)",
            "order_id": order_id
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating order: {str(e)}")


@app.get("/orders")
async def list_orders(limit: int = 50):
    """Return most recent orders (for UI list view)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT order_id, status, payload, kafka_metadata, created_at, updated_at
            FROM orders
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        results = []
        for row in rows:
            payload = row[2]
            if isinstance(payload, str):
                payload = json.loads(payload)

            kafka_metadata = row[3]
            if isinstance(kafka_metadata, str):
                kafka_metadata = json.loads(kafka_metadata)

            results.append(
                {
                    "order_id": row[0],
                    "status": row[1],
                    "payload": payload,
                    "kafka_metadata": kafka_metadata,
                    "created_at": row[4].isoformat() if row[4] else None,
                    "updated_at": row[5].isoformat() if row[5] else None,
                }
            )

        return {"orders": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing orders: {str(e)}")


@app.options("/orders")
async def options_orders():
    """Handle CORS preflight for /orders."""
    return Response(status_code=200)


@app.websocket("/ws/orders")
async def websocket_orders(websocket: WebSocket):
    """
    WebSocket endpoint for real-time order streaming.
    
    Clients connect here and receive live order updates from Kafka.
    Similar to tracker.py but streams to browsers instead of stdout.
    """
    await websocket.accept()
    active_connections.append(websocket)
    print(f"✨ WebSocket client connected. Total clients: {len(active_connections)}")
    
    # Start broadcast processor on first connection
    if len(active_connections) == 1:
        asyncio.create_task(broadcast_queue_processor())
    
    try:
        # Keep connection alive
        while True:
            # Wait for any message from client (for keep-alive)
            data = await websocket.receive_text()
    
    except WebSocketDisconnect:
        active_connections.remove(websocket)
        print(f"❌ WebSocket client disconnected. Total clients: {len(active_connections)}")
    except Exception as e:
        print(f"WebSocket error: {e}")
        try:
            active_connections.remove(websocket)
        except ValueError:
            pass


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "websocket_clients": len(active_connections)}


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    """Retrieve order details and status from database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            SELECT order_id, status, payload, kafka_metadata, created_at, updated_at
            FROM orders
            WHERE order_id = %s
            """,
            (order_id,)
        )
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
        
        # Parse payload and kafka_metadata - handle both dict and string formats
        payload = row[2]
        if isinstance(payload, str):
            payload = json.loads(payload)
        
        kafka_metadata = row[3]
        if isinstance(kafka_metadata, str):
            kafka_metadata = json.loads(kafka_metadata)
        
        return {
            "order_id": row[0],
            "status": row[1],
            "payload": payload,
            "kafka_metadata": kafka_metadata,
            "created_at": row[4].isoformat() if row[4] else None,
            "updated_at": row[5].isoformat() if row[5] else None
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching order: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
