import json
import csv
import os
import signal
import sys
from confluent_kafka import Consumer

# File to save the exported data
CSV_FILE = "research_results.csv"

def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "csv-exporter",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["cache-telemetry"])

    print(f"📡 Telemetry Exporter started. Listening for Kafka events...")
    print(f"💾 Saving data to: {CSV_FILE}")

    # Check if file exists to write header
    write_header = not os.path.exists(CSV_FILE)
    
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "Timestamp", "Query", "Model", "Exact_Hit", "Semantic_Hit", 
                "Distance_Score", "Trad_Latency_Ms", "Sem_Latency_Ms", 
                "Total_Latency_Ms", "Shannon_Thresh", "Model_Thresh"
            ])

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                payload = json.loads(msg.value().decode("utf-8"))
                
                # Extract fields with safe defaults
                row = [
                    payload.get("timestamp", ""),
                    payload.get("query", ""),
                    payload.get("model", ""),
                    payload.get("exact_match_hit", False),
                    payload.get("semantic_match_hit", False),
                    payload.get("distance_score", 0.0),
                    payload.get("trad_latency_ms", 0.0),
                    payload.get("sem_latency_ms", 0.0),
                    payload.get("total_latency_ms", 0.0),
                    payload.get("shannon_thresh", 0.0),
                    payload.get("model_thresh", 0.0)
                ]
                
                writer.writerow(row)
                f.flush()  # Force write to disk immediately
                print(f"✅ Logged query: '{payload.get('query', '')[:30]}...' (Model: {payload.get('model', 'unknown')})")

        except KeyboardInterrupt:
            print("\n🛑 Exporter stopped by user.")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
