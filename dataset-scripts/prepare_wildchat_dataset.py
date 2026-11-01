"""Download and prepare an English, single-turn WildChat benchmark corpus.

The output deliberately matches benchmark.py's required CSV schema:
``query,category``.  It keeps only the first user message in each
conversation so every row is independently runnable through the gRPC gateway.
Raw source shards are stored under data/raw/wildchat and are intentionally
ignored by Git.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
import re
import subprocess
from pathlib import Path

import pyarrow.parquet as pq


DATASET_REPO = "allenai/WildChat"
REVISION = "main"
SHARDS = [f"train-{index:05d}-of-00006.parquet" for index in range(6)]
BASE_URL = f"https://huggingface.co/datasets/{DATASET_REPO}/resolve/{REVISION}/data"
WHITESPACE = re.compile(r"\s+")
MATH_MARKERS = re.compile(r"\b(math|algebra|calculus|equation|integral|derivative|geometry|probability|statistics)\b|[=^]", re.I)
TECHNICAL_MARKERS = re.compile(
    r"\b(code|program|python|javascript|java|c\+\+|golang|rust|sql|api|docker|kubernetes|server|database|linux|debug|algorithm|software)\b",
    re.I,
)
CONVERSATIONAL_MARKERS = re.compile(r"^(hi|hello|hey|what are you|how are you|thank you|thanks)\b", re.I)


def download(url: str, destination: Path) -> None:
    """Download one immutable source shard, atomically when possible."""
    temporary = destination.with_suffix(destination.suffix + ".part")
    subprocess.run(
        ["curl.exe", "--fail", "--location", "--retry", "3", "--output", str(temporary), url],
        check=True,
    )
    temporary.replace(destination)


def normalise(text: str) -> str:
    return WHITESPACE.sub(" ", text).strip()


def category_for(query: str) -> str:
    if MATH_MARKERS.search(query):
        return "math"
    if TECHNICAL_MARKERS.search(query):
        return "technical"
    if CONVERSATIONAL_MARKERS.search(query):
        return "conversational"
    return "general"


def first_safe_user_message(conversation: list[dict]) -> str | None:
    for message in conversation:
        if message.get("role") != "user":
            continue
        if message.get("redacted") or message.get("toxic"):
            return None
        content = message.get("content")
        return normalise(content) if isinstance(content, str) else None
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare a WildChat corpus for FSCgRPC benchmarking.")
    parser.add_argument("--output", type=Path, default=Path("data/wildchat_clean.csv"))
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/wildchat"))
    parser.add_argument("--limit", type=int, default=1_000, help="Number of deterministic sampled prompts to retain.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-chars", type=int, default=20)
    parser.add_argument("--max-chars", type=int, default=500)
    parser.add_argument("--skip-download", action="store_true", help="Use already-downloaded parquet shards.")
    args = parser.parse_args()
    if args.limit < 1 or args.min_chars < 1 or args.max_chars < args.min_chars:
        parser.error("limit must be positive and min-chars must be <= max-chars")

    args.raw_dir.mkdir(parents=True, exist_ok=True)
    shard_paths: list[Path] = []
    for shard in SHARDS:
        path = args.raw_dir / shard
        if not path.exists():
            if args.skip_download:
                parser.error(f"missing source shard: {path}")
            print(f"Downloading {shard}...")
            download(f"{BASE_URL}/{shard}?download=true", path)
        shard_paths.append(path)

    candidates: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    counts = {"rows": 0, "top_level_unsafe": 0, "invalid_or_unsafe_message": 0, "length": 0, "duplicate": 0}
    columns = ["conversation_id", "language", "toxic", "redacted", "conversation"]
    for path in shard_paths:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=2_048, columns=columns):
            for row in batch.to_pylist():
                counts["rows"] += 1
                if row["language"] != "English" or row["toxic"] or row["redacted"]:
                    counts["top_level_unsafe"] += 1
                    continue
                query = first_safe_user_message(row["conversation"])
                if query is None:
                    counts["invalid_or_unsafe_message"] += 1
                    continue
                if not args.min_chars <= len(query) <= args.max_chars:
                    counts["length"] += 1
                    continue
                key = query.casefold()
                if key in seen:
                    counts["duplicate"] += 1
                    continue
                seen.add(key)
                candidates.append((query, category_for(query), row["conversation_id"]))

    if len(candidates) < args.limit:
        parser.error(f"only {len(candidates)} prompts passed the filters; requested {args.limit}")

    random.Random(args.seed).shuffle(candidates)
    selected = candidates[: args.limit]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["query", "category"])
        writer.writerows((query, category) for query, category, _ in selected)

    fingerprint = hashlib.sha256(args.output.read_bytes()).hexdigest()
    category_counts: dict[str, int] = {}
    for _, category, _ in selected:
        category_counts[category] = category_counts.get(category, 0) + 1
    print(f"Wrote {len(selected)} prompts to {args.output}")
    print(f"Category counts: {category_counts}")
    print(f"Source rows: {counts['rows']}; filtered: {counts}; output SHA-256: {fingerprint}")


if __name__ == "__main__":
    main()
