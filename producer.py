import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
import json
import uuid
import time
import datetime

load_dotenv()

try:
    connection = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432",
        database=os.getenv("DB_NAME")
    )
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print("Conectado ao PostgreSQL com sucesso!")
except Exception as e:
    print(f"Erro ao conectar no banco: {e}")
    exit(1)

producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

cursor.execute("SELECT * FROM marts.fct_orders LIMIT 100")
orders = cursor.fetchall()

print(f"🚀 Iniciando a simulação de {len(orders)} pedidos...")

try:
    for order in orders:
        eventos = [
            "order_placed",
            "order_shipped",
            "order_delivered"
        ]

        for event_type in eventos:

            evento = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "order_id": order['order_id'],
                "customer_id": order['customer_sk'],
                "total_value": float(order['payment_value']), 
                "event_timestamp": datetime.datetime.now().isoformat()
            }

            producer.send("olist.order_events", evento)

            time.sleep(0.1)

except KeyboardInterrupt:
    print("\nSimulação interrompida pelo usuário (Ctrl+C).")

finally:
    print("⏳ Aguardando o envio das mensagens no buffer...")
    producer.flush()
    producer.close()
    cursor.close()
    connection.close()
    print("✅ Desligamento concluído. Bye!")