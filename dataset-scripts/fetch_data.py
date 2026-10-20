#!/usr/bin/env python3
"""
fetch_datasets.py - download REAL query datasets and write them as 2-column CSVs: query,category
(the format your benchmark.py already reads; row order = replay order).

  python fetch_datasets.py --install          # pip install the needed libraries
  python fetch_datasets.py qqp                # Quora Question Pairs  -> data/qqp.csv
  python fetch_datasets.py askubuntu          # Ask Ubuntu duplicates -> data/askubuntu.csv
  python fetch_datasets.py wildchat           # WildChat (gated)      -> data/wildchat.csv
  python fetch_datasets.py aol --day 2006-03-15   # AOL query log (one day) -> data/aol.csv
  python fetch_datasets.py all
  python fetch_datasets.py --selftest         # offline check of the conversion logic

`category` meaning
  qqp / askubuntu : duplicate-group id (human-marked duplicates share a category; every other
                    question has its own unique category)  -> usable as ground truth
  wildchat / aol  : the single value "unlabeled" (no ground truth; review sampled hits by hand)
"""

import argparse
import csv
import gzip
import os
import random
import re
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta

OUT_DIR = "data"
RAW_DIR = os.path.join(OUT_DIR, "raw")
ASKUBUNTU_BASE = "https://raw.githubusercontent.com/taolei87/askubuntu/master/"
ASKUBUNTU_FILES = ["texts_raw_fixed.txt.gz", "train_random.txt", "dev.txt", "test.txt"]


# ----------------------------------------------------------------------------- helpers
def clean(s):
    return re.sub(r"\s+", " ", str(s)).strip()


def write_csv(path, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write("query,category\n")
        w = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
        for q, c in rows:
            w.writerow([q, c])


def report(name, rows, path):
    n = len(rows)
    uniq = len({q for q, _ in rows})
    cats = len({c for _, c in rows})
    print(
        f"[{name}] wrote {path}: {n} rows | {uniq} unique | exact-repeat rate "
        f"{100 * (1 - uniq / max(n, 1)):.1f}% | {cats} categories"
    )


class UnionFind:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        self.p[self.find(a)] = self.find(b)


def cluster_rows(texts, edges, prefix, limit, singleton_ratio, seed):
    """texts: {id: text}; edges: [(id_a, id_b)] human-marked duplicates.
    Keeps whole duplicate groups (size>=2) plus a sample of singletons, shuffled."""
    rng = random.Random(seed)
    uf = UnionFind()
    for a, b in edges:
        if a in texts and b in texts:
            uf.union(a, b)
    groups = {}
    for i in texts:
        groups.setdefault(uf.find(i), []).append(i)
    multi = [g for g in groups.values() if len(g) >= 2]
    singles = [g[0] for g in groups.values() if len(g) == 1]
    rng.shuffle(multi)
    rng.shuffle(singles)

    budget = int(limit / (1 + singleton_ratio))
    chosen, count = [], 0
    for g in multi:
        if count + len(g) > budget and chosen:
            break
        chosen.append(g)
        count += len(g)
    rows = []
    for k, g in enumerate(chosen):
        rows += [(clean(texts[i]), f"{prefix}_dup{k}") for i in g]
    for i in singles[: int(count * singleton_ratio)]:
        rows.append((clean(texts[i]), f"{prefix}_single{i}"))
    rng.shuffle(rows)
    return rows[:limit]


# ----------------------------------------------------------------------------- builders (pure)
def build_qqp(pairs, limit, singleton_ratio, seed):
    """pairs: iterable of (question1, question2, label)."""
    ids, texts, edges = {}, {}, []

    def get_id(t):
        t = clean(t)
        if t not in ids:
            ids[t] = len(ids)
            texts[ids[t]] = t
        return ids[t]

    for q1, q2, label in pairs:
        a, b = get_id(q1), get_id(q2)
        if int(label) == 1 and a != b:
            edges.append((a, b))
    return cluster_rows(texts, edges, "qqp", limit, singleton_ratio, seed)


def build_askubuntu(corpus_lines, pair_lines, limit, singleton_ratio, seed):
    """corpus: 'id<TAB>title<TAB>body' ; pairs: 'qid<TAB>similar ids (space sep)<TAB>...'."""
    texts = {}
    for line in corpus_lines:
        parts = line.rstrip("\n").split("\t")
        if len(parts) >= 2 and parts[1].strip():
            texts[parts[0]] = parts[1]
    edges = []
    for line in pair_lines:
        parts = line.rstrip("\n").split("\t")
        if len(parts) >= 2:
            edges += [(parts[0], s) for s in parts[1].split() if s]
    return cluster_rows(texts, edges, "au", limit, singleton_ratio, seed)


def build_wildchat(records, limit):
    """records: dicts with conversation[list of {role,content,...}], timestamp, language, toxic, redacted."""
    kept = []
    for r in records:
        if r.get("language") != "English" or r.get("toxic") or r.get("redacted"):
            continue
        conv = r.get("conversation") or []
        first = next((m for m in conv if m.get("role") == "user"), None)
        if not first:
            continue
        q = clean(first.get("content", ""))
        if 3 <= len(q) <= 500:
            kept.append((str(r.get("timestamp", "")), q))
    kept.sort(key=lambda x: x[0])  # replay in time order
    return [(q, "unlabeled") for _, q in kept[:limit]]


def parse_ts(t):
    try:
        return datetime.fromisoformat(str(t))
    except ValueError:
        return None


def build_aol(records, day, limit, same_user_minutes):
    """records: iterable of (user_id, query, time). Time-ordered; optionally drops a user's own
    repeat of the same query within N minutes (browser-level re-submits / 'next page')."""
    kept = []
    for user, q, t in records:
        q = clean(q)
        if len(q) < 2 or (day and not str(t).startswith(day)):
            continue
        kept.append((str(t), user, q))
    kept.sort(key=lambda x: x[0])
    rows, last_seen = [], {}
    for t, user, q in kept:
        ts = parse_ts(t)
        if same_user_minutes and ts is not None:
            prev = last_seen.get((user, q))
            last_seen[(user, q)] = ts
            if prev is not None and ts - prev < timedelta(minutes=same_user_minutes):
                continue
        rows.append((q, "unlabeled"))
    return rows[:limit]


# ----------------------------------------------------------------------------- downloaders
def fetch_qqp(a):
    from datasets import load_dataset

    try:
        ds = load_dataset("nyu-mll/glue", "qqp")
    except Exception:
        ds = load_dataset("glue", "qqp")
    pairs = []
    for split in ("train", "validation"):
        pairs += [(r["question1"], r["question2"], r["label"]) for r in ds[split]]
    return build_qqp(pairs, a.limit, a.singleton_ratio, a.seed)


def fetch_askubuntu(a):
    os.makedirs(RAW_DIR, exist_ok=True)
    for name in ASKUBUNTU_FILES:
        dest = os.path.join(RAW_DIR, "askubuntu_" + name)
        if not os.path.exists(dest):
            print("downloading", name)
            urllib.request.urlretrieve(ASKUBUNTU_BASE + name, dest)
    with gzip.open(
        os.path.join(RAW_DIR, "askubuntu_texts_raw_fixed.txt.gz"),
        "rt",
        encoding="utf-8",
        errors="replace",
    ) as f:
        corpus = f.readlines()
    pairs = []
    for name in ("train_random.txt", "dev.txt", "test.txt"):
        with open(os.path.join(RAW_DIR, "askubuntu_" + name), encoding="utf-8") as f:
            pairs += f.readlines()
    return build_askubuntu(corpus, pairs, a.limit, a.singleton_ratio, a.seed)


def fetch_wildchat(a):
    from datasets import load_dataset

    print(
        "WildChat is gated: accept its license on Hugging Face and run `huggingface-cli login` "
        "(or set HF_TOKEN) first."
    )
    ds = load_dataset(a.wildchat_name, split="train", streaming=True)

    def scan():
        for i, r in enumerate(ds):
            if i >= a.scan:
                break
            yield r

    return build_wildchat(scan(), a.limit)


def fetch_aol(a):
    import ir_datasets

    print(
        "Loading aol-ia through ir_datasets (first run downloads a large file from the Internet "
        "Archive; the log is for NON-COMMERCIAL research use)."
    )
    ds = ir_datasets.load("aol-ia")

    def records():
        for rec in ds.qlogs_iter():
            q = getattr(rec, "query", None) or getattr(rec, "query_orig", "")
            yield (getattr(rec, "user_id", ""), q, getattr(rec, "time", ""))

    return build_aol(records(), a.day, a.limit, a.same_user_minutes)


JOBS = {
    "qqp": fetch_qqp,
    "askubuntu": fetch_askubuntu,
    "wildchat": fetch_wildchat,
    "aol": fetch_aol,
}


# ----------------------------------------------------------------------------- selftest
def selftest():
    qqp = build_qqp(
        [
            ("How do I learn Go?", "What's the best way to learn Golang?", 1),
            ("What's the best way to learn Golang?", "How to start with Go?", 1),
            ("Why is the sky blue?", "How tall is Everest?", 0),
        ],
        100,
        1.0,
        1,
    )
    cats = {}
    for q, c in qqp:
        cats.setdefault(c, []).append(q)
    assert any(len(v) == 3 for v in cats.values()), (
        "transitive duplicate group expected"
    )

    au = build_askubuntu(
        [
            "1\thow to install ubuntu\tbody\n",
            "2\tinstalling ubuntu on pc\tbody\n",
            "3\twifi not working\tbody\n",
        ],
        ["1\t2\t3\n"],
        100,
        1.0,
        1,
    )
    assert len({c for _, c in au if c.startswith("au_dup")}) == 1

    wc = build_wildchat(
        [
            {
                "language": "English",
                "toxic": False,
                "redacted": False,
                "timestamp": "2023-05-02",
                "conversation": [{"role": "user", "content": "second\nquery"}],
            },
            {
                "language": "English",
                "toxic": False,
                "redacted": False,
                "timestamp": "2023-05-01",
                "conversation": [{"role": "user", "content": "first query"}],
            },
            {
                "language": "French",
                "toxic": False,
                "redacted": False,
                "timestamp": "2023-05-03",
                "conversation": [{"role": "user", "content": "bonjour"}],
            },
        ],
        10,
    )
    assert [q for q, _ in wc] == ["first query", "second query"], wc

    aol = build_aol(
        [
            ("1", "cheap flights", "2006-03-15 10:00:00"),
            (
                "1",
                "cheap flights",
                "2006-03-15 10:05:00",
            ),  # same user, 5 min -> dropped
            ("2", "cheap flights", "2006-03-15 10:06:00"),  # different user -> kept
            (
                "1",
                "cheap flights",
                "2006-03-15 12:00:00",
            ),  # same user, 2h later -> kept
            ("3", "other day", "2006-03-16 09:00:00"),
        ],
        "2006-03-15",
        100,
        30,
    )
    assert [q for q, _ in aol] == ["cheap flights"] * 3, aol

    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "t.csv")
        write_csv(p, qqp)
        with open(p, encoding="utf-8") as f:
            reader = csv.DictReader(f)  # exactly how benchmark.py reads it
            assert reader.fieldnames == ["query", "category"]
            assert len([r["query"] for r in reader if r["query"].strip()]) == len(qqp)
    print("selftest OK (conversion logic only; downloads are NOT exercised)")


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("dataset", nargs="?", choices=list(JOBS) + ["all"])
    ap.add_argument(
        "--install", action="store_true", help="pip install datasets ir_datasets"
    )
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--limit", type=int, default=2000, help="max rows per output file")
    ap.add_argument(
        "--singleton-ratio",
        type=float,
        default=1.0,
        help="qqp/askubuntu: unmatched questions added per duplicate-group member",
    )
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--scan", type=int, default=100000, help="wildchat: rows to scan")
    ap.add_argument("--wildchat-name", default="allenai/WildChat-1M")
    ap.add_argument(
        "--day", default="2006-03-15", help="aol: single day to replay (YYYY-MM-DD)"
    )
    ap.add_argument(
        "--same-user-minutes",
        type=int,
        default=30,
        help="aol: drop a user's repeat of their own query within N minutes (0 = keep)",
    )
    ap.add_argument("--out-dir", default=OUT_DIR)
    a = ap.parse_args()

    if a.selftest:
        return selftest()
    if a.install:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "datasets",
                "ir_datasets",
                "huggingface_hub",
            ]
        )
        if not a.dataset:
            return
    if not a.dataset:
        ap.print_help()
        return
    for name in JOBS if a.dataset == "all" else [a.dataset]:
        try:
            rows = JOBS[name](a)
        except Exception as e:
            print(f"[{name}] FAILED: {type(e).__name__}: {e}")
            continue
        path = os.path.join(a.out_dir, f"{name}.csv")
        write_csv(path, rows)
        report(name, rows, path)


if __name__ == "__main__":
    main()
