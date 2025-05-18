from confluent_kafka import Consumer, KafkaException
import psycopg2
import json
import sys

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "csv-processor-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}
KAFKA_TOPIC = "csv_records"

class DatabaseProcessor:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="your_db",
            user="your_user",
            password="your_password",
            host="your_host"
        )
    
    def process_chunk(self, chunk):
        try:
            with self.conn.cursor() as cur:
                # Call stored procedure
                cur.callproc(
                    "process_csv_records",
                    (json.dumps(chunk["records"]),)
                
                # Update status to completed
                record_ids = chunk["record_ids"]
                cur.execute("""
                    UPDATE records
                    SET status = 'completed', completed_at = NOW()
                    WHERE record_id = ANY(%s)
                """, (record_ids,))
                self.conn.commit()
                return True
        except Exception as e:
            print(f"Database error: {str(e)}")
            self.conn.rollback()
            return False

def main():
    consumer = Consumer(KAFKA_CONFIG)
    processor = DatabaseProcessor()

    try:
        consumer.subscribe([KAFKA_TOPIC])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                chunk = json.loads(msg.value().decode("utf-8"))
                print(f"Processing chunk {chunk['metadata']['chunk_id']}")
                
                if processor.process_chunk(chunk):
                    consumer.commit(msg)
                    print(f"Completed chunk {chunk['metadata']['chunk_id']}")
                else:
                    print(f"Failed chunk {chunk['metadata']['chunk_id']}")
                    # Implement dead-letter queue logic here
            except Exception as e:
                print(f"Processing error: {str(e)}")
    finally:
        consumer.close()
        processor.conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)