"""Persist the independent Laya cache-audit Kafka topic to CSV."""

import csv
import json
import os

from confluent_kafka import Consumer

TOPIC = "laya-cache-audit"
CSV_PATH = os.path.join("results", "laya_cache_audit_v2.csv")
FIELDS = [
    "timestamp",
    "query",
    "model",
    "final_response",
    "exact_match_hit",
    "semantic_match_hit",
    "distance_score",
    "shannon_threshold",
    "model_threshold",
    "cached_query",
    "cached_response",
    "laya_called",
    "laya_probability",
    "laya_threshold",
    "laya_decision",
    "laya_latency_ms",
    "shannon_accepted",
    "candidate_eligible",
    "decision_policy",
    "verifier_error",
    "total_latency_ms",
    "payload_json",
]


def main():
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    write_header = not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0

    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "laya-audit-storage-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    print(f"Persisting {TOPIC} to {CSV_PATH}")

    try:
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=FIELDS)
            if write_header:
                writer.writeheader()
                file.flush()

            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f"Consumer error: {message.error()}")
                    continue

                payload = json.loads(message.value().decode("utf-8"))
                row = {field: payload.get(field, "") for field in FIELDS}
                row["payload_json"] = json.dumps(payload, ensure_ascii=False)
                writer.writerow(row)
                file.flush()
                consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Laya audit storage stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
