
"""
Download and prepare Amazon Product Q&A for FSCgRPC semantic-cache benchmarks.

Output CSV schema:
    query,category
"""

from __future__ import annotations

import argparse
import ast
import csv
import gzip
import hashlib
import json
import re
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import Any

BASE_URL = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon/qa"

CATEGORY_FILES = {
    "appliances": "qa_Appliances.json.gz",
    "arts-crafts-and-sewing": "qa_Arts_Crafts_and_Sewing.json.gz",
    "automotive": "qa_Automotive.json.gz",
    "baby": "qa_Baby.json.gz",
    "beauty": "qa_Beauty.json.gz",
    "cell-phones-and-accessories": "qa_Cell_Phones_and_Accessories.json.gz",
    "clothing-shoes-and-jewelry": "qa_Clothing_Shoes_and_Jewelry.json.gz",
    "electronics": "qa_Electronics.json.gz",
    "grocery-and-gourmet-food": "qa_Grocery_and_Gourmet_Food.json.gz",
    "health-and-personal-care": "qa_Health_and_Personal_Care.json.gz",
    "home-and-kitchen": "qa_Home_and_Kitchen.json.gz",
    "industrial-and-scientific": "qa_Industrial_and_Scientific.json.gz",
    "musical-instruments": "qa_Musical_Instruments.json.gz",
    "office-products": "qa_Office_Products.json.gz",
    "patio-lawn-and-garden": "qa_Patio_Lawn_and_Garden.json.gz",
    "pet-supplies": "qa_Pet_Supplies.json.gz",
    "software": "qa_Software.json.gz",
    "sports-and-outdoors": "qa_Sports_and_Outdoors.json.gz",
    "tools-and-home-improvement": "qa_Tools_and_Home_Improvement.json.gz",
    "toys-and-games": "qa_Toys_and_Games.json.gz",
    "video-games": "qa_Video_Games.json.gz",
}

WHITESPACE = re.compile(r"\s+")


def normalise(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return WHITESPACE.sub(" ", value).strip()


def download(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DatasetPrep/1.0)"},
    )

    print(f"Downloading: {url}")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            with temporary.open("wb") as output:
                shutil.copyfileobj(response, output, length=1024 * 1024)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    temporary.replace(destination)
    print(f"Downloaded {destination.stat().st_size / (1024**2):.1f} MiB")


def parse_record(line: str) -> dict[str, Any] | None:
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        try:
            value = ast.literal_eval(line)
        except (ValueError, SyntaxError):
            return None
    return value if isinstance(value, dict) else None


def get_archive(args: argparse.Namespace) -> Path:
    filename = CATEGORY_FILES[args.category]
    archive = args.raw_dir / filename
    args.raw_dir.mkdir(parents=True, exist_ok=True)

    source_url = args.source_url or f"{BASE_URL}/{filename}"

    if not archive.exists():
        if args.skip_download:
            raise FileNotFoundError(f"Archive not found: {archive}")
        download(source_url, archive)
    else:
        print(f"Using existing archive: {archive}")

    return archive


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare Amazon Q&A prompts for semantic-cache benchmarks."
    )

    parser.add_argument(
        "--category",
        choices=sorted(CATEGORY_FILES),
        default="electronics",
    )
    parser.add_argument(
        "--product-id",
        help="Optional ASIN to filter one product. Omit for the entire category.",
    )
    parser.add_argument("--source-url")
    parser.add_argument("--output", type=Path, default=Path("dataset.csv"))
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/amazon_qa"))
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum prompts to export; 0 means no limit (default: 0).",
    )
    parser.add_argument("--min-chars", type=int, default=10)
    parser.add_argument("--max-chars", type=int, default=500)
    parser.add_argument("--skip-download", action="store_true")

    args = parser.parse_args()

    if args.limit < 0:
        parser.error("--limit must be 0 or greater")
    if args.min_chars < 1 or args.max_chars < args.min_chars:
        parser.error("--min-chars must be positive and <= --max-chars")

    try:
        archive = get_archive(args)
    except Exception as exc:
        parser.error(str(exc))

    target_asin = (
        args.product_id.strip().casefold()
        if args.product_id
        else None
    )

    counts = {
        "records_scanned": 0,
        "product_matches": 0,
        "missing_question": 0,
        "length_filtered": 0,
        "duplicate": 0,
        "malformed": 0,
    }

    seen: set[str] = set()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"Preparing {args.category}: "
        f"{'ASIN ' + args.product_id if target_asin else 'ALL PRODUCTS'}"
    )

    # Stream records to avoid holding the full category in memory.
    with gzip.open(archive, "rt", encoding="utf-8", errors="replace") as source:
        with args.output.open("w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output)
            writer.writerow(["query", "category"])

            written = 0

            for line in source:
                if not line.strip():
                    continue

                record = parse_record(line)
                if record is None:
                    counts["malformed"] += 1
                    continue

                counts["records_scanned"] += 1

                asin = normalise(str(record.get("asin", ""))).casefold()

                # If no product ID was provided, include every product.
                if target_asin and asin != target_asin:
                    continue

                counts["product_matches"] += 1

                question = normalise(record.get("question"))
                if not question:
                    counts["missing_question"] += 1
                    continue

                if not args.min_chars <= len(question) <= args.max_chars:
                    counts["length_filtered"] += 1
                    continue

                key = question.casefold()
                if key in seen:
                    counts["duplicate"] += 1
                    continue

                seen.add(key)
                writer.writerow([question, "product_qa"])
                written += 1

                if args.limit and written >= args.limit:
                    break

    if written == 0:
        args.output.unlink(missing_ok=True)
        print("No usable questions found. Check the category, ASIN, and filters.")
        print(f"Counts: {counts}")
        sys.exit(2)

    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()

    print(f"\nWrote {written} prompts to {args.output}")
    print(f"Mode: {'single product' if target_asin else 'entire category'}")
    print(f"Counts: {counts}")
    print(f"Output SHA-256: {digest}")


if __name__ == "__main__":
    main()
