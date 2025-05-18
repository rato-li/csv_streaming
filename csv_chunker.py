import csv
import uuid
from datetime import datetime
from pathlib import Path
import requests

CHUNK_SIZE = 100  # Parameterized chunk size
API_ENDPOINT = "http://localhost:8000/enqueue"

def process_csv(file_path):
    chunk = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            # Add processing metadata
            row.update({
                "record_id": str(uuid.uuid4()),
                "status": "ready",
                "created_at": datetime.utcnow().isoformat()
            })
            chunk.append(row)
            
            if len(chunk) >= CHUNK_SIZE:
                send_chunk(chunk)
                chunk = []
        
        if chunk:  # Send remaining records
            send_chunk(chunk)

def send_chunk(chunk):
    try:
        response = requests.post(
            API_ENDPOINT,
            json={"records": chunk},
            timeout=10
        )
        if response.status_code == 200:
            print(f"Sent chunk with {len(chunk)} records")
        else:
            print(f"Failed to send chunk: {response.text}")
    except Exception as e:
        print(f"Error sending chunk: {str(e)}")

if __name__ == "__main__":
    csv_path = "path/to/your/source.csv"
    process_csv(csv_path)