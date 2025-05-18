import csv
import json
import uuid
from datetime import datetime
from confluent_kafka import Producer
import psycopg2

CHUNK_SIZE = 100  # Parameterized chunk size
KAFKA_TOPIC = "csv_records"
KAFKA_CONFIG = {"bootstrap.servers": "localhost:9092"}

class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="your_db",
            user="your_user",
            password="your_password",
            host="your_host"
        )
    
    def update_records_status(self, record_ids, status):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE records
                SET status = %s, updated_at = NOW()
                WHERE record_id = ANY(%s)
            """, (status, record_ids))
        self.conn.commit()

class KafkaCSVProducer:
    def __init__(self):
        self.producer = Producer(KAFKA_CONFIG)
        self.db = DatabaseManager()

    def delivery_report(self, err, msg):
        if err:
            print(f"Message delivery failed: {err}")
        record_ids = json.loads(msg.value())["record_ids"]
        self.db.update_records_status(record_ids, "ready")
        # Implement retry logic here
        else:
            print(f"Message delivered to {msg.topic()}")

    def process_csv(self, file_path):
        chunk = []
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                record_id = str(uuid.uuid4())
                chunk.append({
                    **row,
                    "record_id": record_id,
                    "status": "ready",
                    "created_at": datetime.utcnow().isoformat()
                })

                if len(chunk) >= CHUNK_SIZE:
                    self._send_chunk(chunk)
                    chunk = []
            
            if chunk:
                self._send_chunk(chunk)

    def _send_chunk(self, chunk):
        record_ids = [rec["record_id"] for rec in chunk]
        try:
            self.producer.produce(
                topic=KAFKA_TOPIC,
                value=json.dumps({
                    "metadata": {
                        "chunk_id": str(uuid.uuid4()),
                        "sent_at": datetime.utcnow().isoformat()
                    },
                    "records": chunk,
                    "record_ids": record_ids
                }),
                callback=self.delivery_report
            )
            self.db.update_records_status(record_ids, "sent")
        except Exception as e:
            print(f"Error sending chunk: {str(e)}")
            self.db.update_records_status(record_ids, "ready")

if __name__ == "__main__":
    producer = KafkaCSVProducer()
    producer.process_csv("path/to/your/source.csv")
    producer.producer.flush()