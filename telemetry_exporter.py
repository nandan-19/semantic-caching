import json
import csv
import os
from confluent_kafka import Consumer

# File to save the exported data
CSV_FILE = "research_results.csv"
OUTPUT_FIELDS = [
    "Timestamp", "Request_ID", "Query", "Candidate_Query", "Model", "Response_Source",
    "Response", "Cached_Response", "Exact_Hit", "Semantic_Hit",
    "Guard_Checked", "Guard_Blocked", "Guard_Reason",
    "Distance_Score", "Trad_Latency_Ms", "Sem_Latency_Ms",
    "Total_Latency_Ms", "Shannon_Thresh", "Model_Thresh",
    "Decider", "Laya_Checked", "Laya_Accepted", "Laya_Decision",
    "Laya_Confidence", "Laya_Latency_Ms",
    "Exact_Cache_Latency_Ms", "Embedding_Latency_Ms", "Shannon_Latency_Ms",
    "Model_Threshold_Latency_Ms", "Vector_Search_Latency_Ms", "Admission_Latency_Ms",
    "Cache_Write_Latency_Ms", "Edge_Generation_Latency_Ms", "Cloud_Fallback_Latency_Ms",
    "Exact_Cache_Status", "Embedding_Status", "Vector_Search_Status", "Admission_Status",
    "Cache_Write_Status", "Edge_Generation_Status", "Cloud_Fallback_Status",
]


def has_expected_schema(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return True

    with open(path, newline="", encoding="utf-8") as existing_file:
        return next(csv.reader(existing_file), []) == OUTPUT_FIELDS


def output_path():
    if has_expected_schema(CSV_FILE):
        return CSV_FILE

    # Keep appends compatible with CSVs created before Laya telemetry existed.
    return "research_results_laya.csv"


def telemetry_row(payload):
    return {
        "Timestamp": payload.get("timestamp", ""),
        "Request_ID": payload.get("request_id", ""),
        "Query": payload.get("query", ""),
        "Candidate_Query": payload.get("candidate_query", ""),
        "Model": payload.get("model", ""),
        "Response_Source": payload.get("response_source", ""),
        "Response": payload.get("response", ""),
        "Cached_Response": payload.get("cached_response", ""),
        "Exact_Hit": payload.get("exact_match_hit", False),
        "Semantic_Hit": payload.get("semantic_match_hit", False),
        "Guard_Checked": payload.get("guard_checked", False),
        "Guard_Blocked": payload.get("guard_blocked", False),
        "Guard_Reason": payload.get("guard_reason", ""),
        "Distance_Score": payload.get("distance_score", 0.0),
        "Trad_Latency_Ms": payload.get("trad_latency_ms", 0.0),
        "Sem_Latency_Ms": payload.get("sem_latency_ms", 0.0),
        "Total_Latency_Ms": payload.get("total_latency_ms", 0.0),
        "Shannon_Thresh": payload.get("shannon_thresh", 0.0),
        "Model_Thresh": payload.get("model_thresh", 0.0),
        "Decider": payload.get("decider", "shannon"),
        "Laya_Checked": payload.get("laya_checked", False),
        "Laya_Accepted": payload.get("laya_accepted", False),
        "Laya_Decision": payload.get("laya_decision", ""),
        "Laya_Confidence": payload.get("laya_confidence", 0.0),
        "Laya_Latency_Ms": payload.get("laya_latency_ms", 0.0),
        "Exact_Cache_Latency_Ms": payload.get("exact_cache_latency_ms", 0.0),
        "Embedding_Latency_Ms": payload.get("embedding_latency_ms", 0.0),
        "Shannon_Latency_Ms": payload.get("shannon_latency_ms", 0.0),
        "Model_Threshold_Latency_Ms": payload.get("model_threshold_latency_ms", 0.0),
        "Vector_Search_Latency_Ms": payload.get("vector_search_latency_ms", 0.0),
        "Admission_Latency_Ms": payload.get("admission_latency_ms", 0.0),
        "Cache_Write_Latency_Ms": payload.get("cache_write_latency_ms", 0.0),
        "Edge_Generation_Latency_Ms": payload.get("edge_generation_latency_ms", 0.0),
        "Cloud_Fallback_Latency_Ms": payload.get("cloud_fallback_latency_ms", 0.0),
        "Exact_Cache_Status": payload.get("exact_cache_status", "skipped"),
        "Embedding_Status": payload.get("embedding_status", "skipped"),
        "Vector_Search_Status": payload.get("vector_search_status", "skipped"),
        "Admission_Status": payload.get("admission_status", "skipped"),
        "Cache_Write_Status": payload.get("cache_write_status", "skipped"),
        "Edge_Generation_Status": payload.get("edge_generation_status", "skipped"),
        "Cloud_Fallback_Status": payload.get("cloud_fallback_status", "skipped"),
    }

def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "csv-exporter",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["cache-telemetry"])

    destination = output_path()
    if not has_expected_schema(destination):
        raise RuntimeError(f"{destination} does not match the Laya telemetry CSV schema")

    print(f"📡 Telemetry Exporter started. Listening for Kafka events...")
    print(f"💾 Saving data to: {destination}")

    write_header = not os.path.exists(destination) or os.path.getsize(destination) == 0
    
    with open(destination, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        if write_header:
            writer.writeheader()

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                payload = json.loads(msg.value().decode("utf-8"))
                
                writer.writerow(telemetry_row(payload))
                f.flush()  # Force write to disk immediately
                print(f"✅ Logged query: '{payload.get('query', '')[:30]}...' (Model: {payload.get('model', 'unknown')})")

        except KeyboardInterrupt:
            print("\n🛑 Exporter stopped by user.")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
