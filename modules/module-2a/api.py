from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import mysql.connector
import pika
import json

app = FastAPI()

class Order(BaseModel):
    product_id: int
    quantity: int = Field(gt=0, description="Quantity must be greater than 0")
    customer_id: int

def get_mysql_connection():
    return mysql.connector.connect(
        host="mysql_db",
        user="root",
        password="root",
        database="webstore"
    )

def publish_to_rabbitmq(order_data, order_id):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='order_queue', durable=True)
    
    message = order_data.copy()
    message['id'] = order_id
    
    channel.basic_publish(
        exchange='',
        routing_key='order_queue',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    connection.close()

@app.post("/api/orders", status_code=status.HTTP_202_ACCEPTED)
async def create_order(order: Order):
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        # Insert into MySQL with PENDING status
        insert_query = """
            INSERT INTO orders (product_id, quantity, customer_id, status)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (order.product_id, order.quantity, order.customer_id, 'PENDING'))
        order_id = cursor.lastrowid
        conn.commit()
        cursor.close()
        conn.close()
        
        # Publish to RabbitMQ
        publish_to_rabbitmq(order.model_dump(), order_id)
        
        return {"message": "Order accepted", "order_id": order_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
