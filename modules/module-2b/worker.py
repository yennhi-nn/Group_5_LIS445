import pika
import time
import json
import psycopg2
import mysql.connector

def get_postgres_connection():
    return psycopg2.connect(
        host="postgres_db",
        user="user",
        password="password",
        database="finance"
    )

def init_postgres_db():
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                order_id INT NOT NULL,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print(" [v] PostgreSQL 'transactions' table checked/created.")
    except Exception as e:
        print(f" [!] Error initializing Postgres: {e}")

def get_mysql_connection():
    return mysql.connector.connect(
        host="mysql_db",
        user="root",
        password="root",
        database="webstore"
    )

def callback(ch, method, properties, body):
    try:
        order_data = json.loads(body)
        print(f" [x] Received order {order_data['id']}")
        
        # Simulate processing (payment)
        time.sleep(2)
        
        amount = order_data['quantity'] * 100 # Simulated amount
        
        # Insert transaction into PostgreSQL
        pg_conn = get_postgres_connection()
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(
            "INSERT INTO transactions (order_id, customer_id, amount, status) VALUES (%s, %s, %s, %s)",
            (order_data['id'], order_data['customer_id'], amount, 'SUCCESS')
        )
        pg_conn.commit()
        pg_cursor.close()
        pg_conn.close()
        
        # Update status in MySQL to COMPLETED
        my_conn = get_mysql_connection()
        my_cursor = my_conn.cursor()
        my_cursor.execute(
            "UPDATE orders SET status = %s WHERE id = %s",
            ('COMPLETED', order_data['id'])
        )
        my_conn.commit()
        my_cursor.close()
        my_conn.close()
        
        print(f" [x] Done processing order {order_data['id']}")
        
        # Send ACK to RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f" [!] Error processing order: {e}")
        # Not sending ACK so the message stays in the queue, or reject it
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    # Initialize PostgreSQL Table
    while True:
        try:
            init_postgres_db()
            break
        except:
            time.sleep(2)
            
    while True:
        try:
            print("Connecting to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600, blocked_connection_timeout=300))
            channel = connection.channel()
            channel.queue_declare(queue='order_queue', durable=True)
            print(' [*] Waiting for messages. To exit press CTRL+C')
            
            # Prefetch count to 1 ensures a worker only gets one message at a time
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='order_queue', on_message_callback=callback)
            
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            print("Connection to RabbitMQ failed, retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()