import json
import subprocess
import time
import argparse
import csv
import sys


def main():
    parser = argparse.ArgumentParser(description="Run FSCgRPC Benchmark Suite")
    parser.add_argument(
        "--model",
        type=str,
        default="qwen2.5:1.5b",
        help="Model name to evaluate (e.g. gemma3:4b, phi3:mini)",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="dataset.csv",
        help="Path to the dataset CSV file",
    )
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between requests in seconds"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit the number of prompts to run"
    )
    parser.add_argument(
        "--offset", type=int, default=0, help="Skip the first N prompts"
    )

    args = parser.parse_args()

    # Load prompts from CSV
    prompts = []
    try:
        with open(args.dataset, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "query" in row and row["query"].strip():
                    prompts.append(row["query"].strip())
    except Exception as e:
        print(f"❌ Failed to load dataset: {e}")
        sys.exit(1)

    if args.offset:
        prompts = prompts[args.offset :]

    if args.limit:
        prompts = prompts[: args.limit]

    if not prompts:
        print("❌ Dataset is empty or invalid.")
        sys.exit(1)

    print(f"🚀 Launching Benchmark Matrix")
    print(f"   Model  : {args.model}")
    print(f"   Queries: {len(prompts)}")
    print(f"   Delay  : {args.delay}s\n")

    for i, query in enumerate(prompts):
        print(f"[{i + 1}/{len(prompts)}] Sending: '{query[:40]}...'")

        cmd = [
            "grpcurl",
            "-plaintext",
            "-import-path",
            "./proto",
            "-proto",
            "api.proto",
            "-H",
            f"x-model-name: {args.model}",
            "-d",
            json.dumps({"text": query}),
            "localhost:50051",
            "semantic_cache.CoreAppService/ProcessQuery",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")

        if result.returncode != 0:
            print(f"  ❌ execution failure: {result.stderr.strip()}")

        time.sleep(args.delay)

    print(
        "\n✅ Benchmark Complete. Results should be captured by telemetry_exporter.py."
    )


if __name__ == "__main__":
    main()
