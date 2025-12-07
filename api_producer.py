import json
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer

app = FastAPI()

# Kafka Producer Configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)


# Request model with validation
class Order(BaseModel):
    user: str = Field(..., min_length=1, description="User identifier")
    product: str = Field(..., min_length=1, description="Product name")
    quantity: int = Field(..., gt=0, description="Quantity must be positive")


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(f"Message value: {msg.value().decode('utf-8')}")


@app.post("/orders")
async def create_order(order: Order):
    """
    Accept an order and produce it to the 'orders' Kafka topic.
    
    Args:
        order: Order object with user, product, and quantity
        
    Returns:
        Order details with assigned order_id
    """
    try:
        # Create order with unique ID
        order_data = {
            'order_id': str(uuid.uuid4()),
            'user': order.user,
            'product': order.product,
            'quantity': order.quantity
        }
        
        # Convert to JSON and encode
        json_str = json.dumps(order_data).encode('utf-8')
        
        # Produce to Kafka
        producer.produce(
            topic='orders',
            value=json_str,
            callback=delivery_report
        )
        
        # Flush to ensure message is sent
        producer.flush()
        
        return {
            "status": "success",
            "message": "Order produced to Kafka",
            "order": order_data
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error producing order: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
