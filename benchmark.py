import json
import subprocess
import time
import argparse
import csv
import sys


# Small gold labels are passed as gRPC metadata so Go telemetry can be matched
# back to the exact dataset row without changing the request protobuf.
LABEL_METADATA = [
    ("sample_id", "x-sample-id"),
    ("category", "x-category"),
    ("domain_category", "x-domain-category"),
    ("topic", "x-topic"),
    ("scenario_id", "x-scenario-id"),
    ("scenario_role", "x-scenario-role"),
    ("expected_cache_outcome", "x-expected-cache-outcome"),
    ("expected_cache_method", "x-expected-cache-method"),
    ("expected_candidate_id", "x-expected-candidate-id"),
    ("expected_candidate_relation", "x-expected-candidate-relation"),
    ("annotation_confidence", "x-annotation-confidence"),
    ("freshness_sensitive", "x-freshness-sensitive"),
    ("answer_stability", "x-answer-stability"),
    ("reuse_risk", "x-reuse-risk"),
]


def main():
    parser = argparse.ArgumentParser(description="Run FSCgRPC Benchmark Suite")
    parser.add_argument(
        "--model",
        type=str,
        default="qwen2.5:1.5b",
        help="Ollama model used for generation fallback",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="dataset.csv",
        help="Path to dataset CSV. Requires a query column; gold-label columns are optional.",
    )
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between requests in seconds"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of prompts to run"
    )
    parser.add_argument(
        "--offset", type=int, default=0, help="Skip first N dataset rows"
    )
    parser.add_argument(
        "--no-label-metadata",
        action="store_true",
        help="Send only the query; do not forward dataset labels to telemetry",
    )
    args = parser.parse_args()

    try:
        with open(args.dataset, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "query" not in reader.fieldnames:
                raise ValueError("CSV must contain a column named exactly 'query'.")
            rows = [row for row in reader if (row.get("query") or "").strip()]
    except Exception as exc:
        print(f"Failed to load dataset: {exc}")
        sys.exit(1)

    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]
    if not rows:
        print("Dataset is empty or contains no non-empty query values.")
        sys.exit(1)

    label_columns = {column for column, _ in LABEL_METADATA}
    available_labels = label_columns.intersection(rows[0].keys())
    print("Launching benchmark")
    print(f"  Dataset: {args.dataset}")
    print(f"  Model: {args.model}")
    print(f"  Queries: {len(rows)}")
    print(
        f"  Gold label metadata: {'off' if args.no_label_metadata else len(available_labels)} columns detected"
    )
    print(f"  Delay: {args.delay}s")

    for index, row in enumerate(rows, start=1):
        query = row["query"].strip()
        print(
            f"[{index}/{len(rows)}] Sending sample={row.get('sample_id', 'n/a')}: {query[:55]!r}"
        )
        cmd = [
            "grpcurl",
            "-plaintext",
            "-import-path",
            "./proto",
            "-proto",
            "api.proto",
            "-H",
            f"x-model-name: {args.model}",
        ]
        if not args.no_label_metadata:
            for column, header in LABEL_METADATA:
                value = str(row.get(column, "") or "").strip()
                if value:
                    # Dataset label fields are short ASCII identifiers/enums, suitable for metadata.
                    cmd.extend(["-H", f"{header}: {value}"])
        cmd.extend(
            [
                "-d",
                json.dumps({"text": query}, ensure_ascii=False),
                "localhost:50051",
                "semantic_cache.CoreAppService/ProcessQuery",
            ]
        )
        result = subprocess.run(
            cmd, capture_output=True, text=True, encoding="utf-8", errors="replace"
        )
        if result.returncode != 0:
            print(f"  grpcurl failed: {result.stderr.strip()}")
        time.sleep(args.delay)

    print(
        "Benchmark complete. Check the telemetry CSV for predictions and expected labels."
    )


if __name__ == "__main__":
    main()
