import psycopg2
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import json
import psycopg2.extras

load_dotenv()

try:
    connection = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432",
        database=os.getenv("DB_NAME")
    )
    cursor = connection.cursor()
    print("Conectado ao PostgreSQL com sucesso!")
except Exception as e:
    print(f"Erro ao conectar no banco: {e}")
    exit(1)

cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS streaming;
                              
    CREATE TABLE IF NOT EXISTS streaming.olist_events (
        event_id UUID PRIMARY KEY,
        event_type VARCHAR,
        order_id VARCHAR,
        customer_id VARCHAR, 
        total_value NUMERIC, 
        event_timestamp TIMESTAMP, 
        ingested_at TIMESTAMP DEFAULT NOW(), 
        kafka_offset BIGINT, 
        kafka_partition INT
    );
""")
connection.commit()

consumer = KafkaConsumer(
    "olist.order_events",
    bootstrap_servers="localhost:19092",
    group_id="grupo_ingestao_batch", 
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Aguardando mensagens no tópico 'olist.order_events'...")

try:
    while True:
        lotes_kafka = consumer.poll(timeout_ms=1000)

        if not lotes_kafka:
            continue

        registros_para_inserir = []

        for particao, mensagens in lotes_kafka.items():
            for mensagem in mensagens:
                evento = mensagem.value
                
                valores = (
                    evento['event_id'],
                    evento['event_type'],
                    evento['order_id'],
                    evento['customer_id'],
                    evento['total_value'],
                    evento['event_timestamp'],
                    mensagem.offset,
                    mensagem.partition
                )
                registros_para_inserir.append(valores)

        query = """
            INSERT INTO streaming.olist_events 
            (event_id, event_type, order_id, customer_id, total_value, event_timestamp, kafka_offset, kafka_partition)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """
        
        psycopg2.extras.execute_batch(cursor, query, registros_para_inserir)
        connection.commit() 
        
        consumer.commit()
        
        print(f"Lote de {len(registros_para_inserir)} eventos inserido no Postgres com sucesso!")

except KeyboardInterrupt:
    print("\nConsumo interrompido pelo usuário (Ctrl+C).")
finally:
    print("Fechando conexões...")
    consumer.close()
    cursor.close()
    connection.close()
    print("Desligamento concluído.")