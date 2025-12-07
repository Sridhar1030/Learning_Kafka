import json
import uuid
import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Load environment variables from .env file FIRST
load_dotenv()

app = FastAPI()

# Kafka Producer Configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

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
    else:
        metadata = {
            'kafka_topic': msg.topic(),
            'kafka_partition': msg.partition(),
            'kafka_offset': msg.offset(),
            'timestamp': datetime.utcnow().isoformat()
        }
        print(f"Message delivered for order {order_id}: {metadata}")
        update_order_status(order_id, 'kafka_published', metadata)


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


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


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
