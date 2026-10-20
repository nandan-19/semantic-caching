"""
Prepare the Customer Support on Twitter dataset for FSCgRPC.

CSV columns default to `query,category`.
--order timestamp: chronological replay (default).
--order category: cluster issue categories, chronological within each.
--order brand: cluster support-account IDs, chronological within each.
--include-metadata adds created_at, brand, tweet_id columns.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import subprocess
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

DATASET_URL = "https://www.kaggle.com/api/v1/datasets/download/thoughtvector/customer-support-on-twitter"
WHITESPACE = re.compile(r"\s+")
URL = re.compile(r"https?://\S+", re.I)
MENTION = re.compile(r"@\w+")
CATEGORY_PATTERNS = {
    "billing": re.compile(r"\b(bill|billing|charge|charged|payment|pay|refund|fee|price|cost)\b", re.I),
    "account": re.compile(r"\b(account|login|log in|password|sign in|verification|locked)\b", re.I),
    "delivery": re.compile(r"\b(delivery|delivered|shipping|ship|order|package|parcel|tracking)\b", re.I),
    "technical": re.compile(r"\b(app|website|site|error|bug|crash|wifi|wi-fi|connection|update)\b", re.I),
}


def download(url: str, destination: Path) -> None:
    temporary = destination.with_suffix(destination.suffix + ".part")
    subprocess.run(
        ["curl.exe", "--fail", "--location", "--retry", "3", "--output", str(temporary), url],
        check=True,
    )
    temporary.replace(destination)


def normalise(text: str) -> str:
    text = URL.sub("", text or "")
    text = MENTION.sub("", text)
    return WHITESPACE.sub(" ", text).strip()


def category_for(query: str) -> str:
    for category, pattern in CATEGORY_PATTERNS.items():
        if pattern.search(query):
            return category
    return "general"


def parse_timestamp(value: str) -> datetime:
    value = (value or "").strip()
    minimum = datetime.min.replace(tzinfo=timezone.utc)
    if not value:
        return minimum
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(value, fmt)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                continue
    return minimum


def is_initial_inbound(row: dict[str, str]) -> bool:
    inbound = (row.get("inbound") or "").strip().lower()
    parent = (row.get("in_response_to_tweet_id") or "").strip()
    return inbound in {"true", "1"} and not parent


def referenced_tweet_ids(row: dict[str, str]) -> set[str]:
    return {
        value.strip()
        for value in (row.get("response_tweet_id") or "").split(",")
        if value.strip()
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare timestamped Twitter support requests for FSCgRPC."
    )
    parser.add_argument("--output", type=Path, default=Path("data/twitter_support_clean.csv"))
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/twitter_support"))
    parser.add_argument("--brand", help="Filter to requests answered by this support account's author_id.")
    parser.add_argument(
        "--order", choices=("timestamp", "category", "brand"), default="timestamp",
        help="Chronological replay, or cluster by category/support account."
    )
    parser.add_argument("--include-metadata", action="store_true")
    parser.add_argument(
        "--deduplicate",
        action="store_true",
        help="Remove repeated normalized query text. By default repeats are preserved for cache-hit evaluation.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Maximum rows; 0 exports all qualifying rows.")
    parser.add_argument("--min-chars", type=int, default=20)
    parser.add_argument("--max-chars", type=int, default=280)
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()

    if args.limit < 0 or args.min_chars < 1 or args.max_chars < args.min_chars:
        parser.error("limit must be >= 0 and min-chars must be <= max-chars")

    args.raw_dir.mkdir(parents=True, exist_ok=True)
    archive = args.raw_dir / "customer-support-on-twitter.zip"
    if not archive.exists():
        if args.skip_download:
            parser.error(f"missing source archive: {archive}")
        print("Downloading Kaggle Customer Support on Twitter archive...")
        download(DATASET_URL, archive)
    else:
        print(f"Using existing archive: {archive}")

    with zipfile.ZipFile(archive) as zipped:
        csv_members = [name for name in zipped.namelist() if name.lower().endswith(".csv")]
        if not csv_members:
            parser.error("source archive contains no CSV files")
        source_csv = max(csv_members, key=lambda name: zipped.getinfo(name).file_size)
        print(f"Reading source CSV: {source_csv}")

        # Map tweet IDs to authors so inbound requests can be grouped by replying account.
        tweet_author: dict[str, str] = {}
        with zipped.open(source_csv) as raw_file:
            reader = csv.DictReader(line.decode("utf-8", errors="replace") for line in raw_file)
            for row in reader:
                tweet_id = (row.get("tweet_id") or "").strip()
                author_id = (row.get("author_id") or "").strip()
                if tweet_id and author_id:
                    tweet_author[tweet_id] = author_id

        candidates: list[dict] = []
        seen: set[str] = set()
        counts: Counter = Counter()

        with zipped.open(source_csv) as raw_file:
            reader = csv.DictReader(line.decode("utf-8", errors="replace") for line in raw_file)
            for row in reader:
                counts["source_rows"] += 1
                if not is_initial_inbound(row):
                    counts["not_initial_inbound"] += 1
                    continue

                raw_text = row.get("text") or ""
                if "__" in raw_text:
                    counts["masked"] += 1
                    continue

                query = normalise(raw_text)
                if not args.min_chars <= len(query) <= args.max_chars:
                    counts["length_filtered"] += 1
                    continue

                key = query.casefold()
                if args.deduplicate and key in seen:
                    counts["duplicate"] += 1
                    continue

                reply_ids = referenced_tweet_ids(row)
                reply_authors = sorted({
                    tweet_author[tweet_id]
                    for tweet_id in reply_ids
                    if tweet_id in tweet_author
                })
                brand = reply_authors[0] if reply_authors else "unknown"

                if args.brand and brand != args.brand.strip():
                    counts["not_brand_routed"] += 1
                    continue

                timestamp_text = (row.get("created_at") or "").strip()
                timestamp = parse_timestamp(timestamp_text)
                if timestamp == datetime.min.replace(tzinfo=timezone.utc):
                    counts["missing_or_invalid_timestamp"] += 1

                seen.add(key)
                candidates.append({
                    "query": query,
                    "category": category_for(query),
                    "created_at": timestamp_text,
                    "_timestamp": timestamp,
                    "brand": brand,
                    "tweet_id": (row.get("tweet_id") or "").strip(),
                })

    if not candidates:
        parser.error("no prompts passed the filters; check the source schema and options")

    if args.order == "timestamp":
        candidates.sort(key=lambda item: (item["_timestamp"], item["tweet_id"]))
    elif args.order == "category":
        candidates.sort(key=lambda item: (item["category"], item["_timestamp"], item["tweet_id"]))
    else:
        candidates.sort(key=lambda item: (item["brand"], item["_timestamp"], item["tweet_id"]))

    if args.limit:
        candidates = candidates[:args.limit]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    columns = ["query", "category"]
    if args.include_metadata:
        columns += ["created_at", "brand", "tweet_id"]

    with args.output.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        for item in candidates:
            writer.writerow({column: item[column] for column in columns})

    print(f"\nWrote {len(candidates)} prompts to {args.output}")
    print(f"Ordering mode: {args.order}")
    print(f"Category counts: {dict(Counter(item['category'] for item in candidates))}")
    print(f"Distinct support-account groups: {len(set(item['brand'] for item in candidates))}")
    print(f"Source/filter counts: {dict(counts)}")
    print(f"Output SHA-256: {hashlib.sha256(args.output.read_bytes()).hexdigest()}")
    if args.order == "timestamp":
        print("Rows are ordered oldest-to-newest by created_at.")
    elif args.order == "category":
        print("Rows are grouped by heuristic category; timestamps are ordered within each group.")
    else:
        print("Rows are grouped by replying support-account author_id; timestamps are ordered within each group.")
    print("Category labels are heuristic labels derived from text, not source annotations.")


if __name__ == "__main__":
    main()
