import json
import redis
import time
from datetime import datetime
import psycopg2  # or your DB connector

class DatabaseProcessor:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="your_db",
            user="your_user",
            password="your_password",
            host="your_host"
        )
    
    def process_records(self, records):
        try:
            cur = self.conn.cursor()
            
            # Convert records to database procedure input
            input_data = [(
                record["record_id"],
                json.dumps(record),
                record["status"]
            ) for record in records]
            
            # Call database procedure
            cur.callproc("process_csv_records", (json.dumps(input_data),))
            
            # Update records to completed
            for record in records:
                record["status"] = "completed"
                record["completed_at"] = datetime.utcnow().isoformat()
            
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"Processing failed: {str(e)}")
            return False
        finally:
            cur.close()

def main():
    r = redis.Redis(host='localhost', port=6379, db=0)
    processor = DatabaseProcessor()
    
    while True:
        try:
            # Blocking pop from queue
            _, chunk_json = r.brpop("processing_queue")
            chunk = json.loads(chunk_json)
            
            print(f"Processing chunk with {len(chunk)} records")
            
            if processor.process_records(chunk):
                print(f"Successfully processed {len(chunk)} records")
            else:
                print(f"Failed to process chunk - requeuing")
                r.lpush("processing_queue", chunk_json)
                
        except Exception as e:
            print(f"Error processing chunk: {str(e)}")
            time.sleep(5)

if __name__ == "__main__":
    main()