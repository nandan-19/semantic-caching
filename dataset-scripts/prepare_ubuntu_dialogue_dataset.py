"""Download and prepare the Ubuntu Dialogue QA corpus for FSCgRPC benchmarks."""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
import re
import subprocess
from pathlib import Path

import pyarrow.parquet as pq


SOURCE_URL = (
    "https://huggingface.co/datasets/sedthh/ubuntu_dialogue_qa/resolve/main/"
    "data/train-00000-of-00001-bb5f874d67d84fd2.parquet?download=true"
)
WHITESPACE = re.compile(r"\s+")


def download(destination: Path) -> None:
    temporary = destination.with_suffix(destination.suffix + ".part")
    subprocess.run(
        ["curl.exe", "--fail", "--location", "--retry", "3", "--output", str(temporary), SOURCE_URL],
        check=True,
    )
    temporary.replace(destination)


def normalise(text: str) -> str:
    return WHITESPACE.sub(" ", text).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare the Ubuntu Dialogue QA corpus for FSCgRPC.")
    parser.add_argument("--output", type=Path, default=Path("data/ubuntu_dialogue_clean.csv"))
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/ubuntu_dialogue"))
    parser.add_argument("--limit", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-chars", type=int, default=20)
    parser.add_argument("--max-chars", type=int, default=500)
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()
    if args.limit < 1 or args.min_chars < 1 or args.max_chars < args.min_chars:
        parser.error("limit must be positive and min-chars must be <= max-chars")

    args.raw_dir.mkdir(parents=True, exist_ok=True)
    source = args.raw_dir / "train-00000-of-00001-bb5f874d67d84fd2.parquet"
    if not source.exists():
        if args.skip_download:
            parser.error(f"missing source shard: {source}")
        print("Downloading Ubuntu Dialogue QA source shard...")
        download(source)

    seen: set[str] = set()
    candidates: list[str] = []
    counts = {"rows": 0, "missing_instruction": 0, "length": 0, "duplicate": 0}
    parquet = pq.ParquetFile(source)
    for batch in parquet.iter_batches(batch_size=2_048, columns=["INSTRUCTION"]):
        for row in batch.to_pylist():
            counts["rows"] += 1
            instruction = row["INSTRUCTION"]
            if not isinstance(instruction, str):
                counts["missing_instruction"] += 1
                continue
            query = normalise(instruction)
            if not args.min_chars <= len(query) <= args.max_chars:
                counts["length"] += 1
                continue
            key = query.casefold()
            if key in seen:
                counts["duplicate"] += 1
                continue
            seen.add(key)
            candidates.append(query)

    if len(candidates) < args.limit:
        parser.error(f"only {len(candidates)} prompts passed the filters; requested {args.limit}")
    random.Random(args.seed).shuffle(candidates)
    selected = candidates[: args.limit]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["query", "category"])
        writer.writerows((query, "technical") for query in selected)

    print(f"Wrote {len(selected)} prompts to {args.output}")
    print(f"Source rows: {counts['rows']}; filtered: {counts}")
    print(f"Output SHA-256: {hashlib.sha256(args.output.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    main()
