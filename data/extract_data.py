"""
analyze_telemetry.py
--------------------
Extracts all metrics required for the research paper from
gemma.csv, llama.csv, qwen.csv telemetry files.

Column format (no header assumed):
  0  timestamp
  1  query
  2  model
  3  track_a_hit       (True/False)
  4  track_b_hit       (True/False)
  5  cosine_distance   (float, 2.0 = sentinel for Track A / no vector search)
  6  embedding_ms      (float)
  7  llm_ms            (float)
  8  total_ms          (float)
  9  shannon_threshold (float)
  10 model_ast_threshold (float)

Run from the directory containing the three CSV files:
  python analyze_telemetry.py
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path

# ── Column names ────────────────────────────────────────────────────────────
COLS = [
    "timestamp",
    "query",
    "model",
    "track_a_hit",
    "track_b_hit",
    "cosine_distance",
    "embedding_ms",
    "llm_ms",
    "total_ms",
    "shannon_threshold",
    "model_ast_threshold",
]

FILES = {
    "qwen2.5:1.5b": "qwen.csv",
    "gemma2:2b": "gemma.csv",
    "llama3.2:3b": "llama.csv",
}

COSINE_SENTINEL = 2.0  # value written when Track A fires (no vector search)


# ── Load ─────────────────────────────────────────────────────────────────────
def load_files() -> pd.DataFrame:
    frames = []

    numeric_cols = [
        "cosine_distance",
        "embedding_ms",
        "llm_ms",
        "total_ms",
        "shannon_threshold",
        "model_ast_threshold",
    ]

    for model_label, fname in FILES.items():
        path = Path(fname)

        if not path.exists():
            print(f"  [WARN] {fname} not found — skipping")
            continue

        # Skip CSV header row
        df = pd.read_csv(
            path,
            header=None,
            names=COLS,
            skiprows=1,
        )

        # Remove accidental cross-model contamination
        df = df[df["model"].astype(str).str.strip() == model_label]

        # Normalise booleans
        df["track_a_hit"] = (
            df["track_a_hit"].astype(str).str.strip().str.lower().eq("true")
        )

        df["track_b_hit"] = (
            df["track_b_hit"].astype(str).str.strip().str.lower().eq("true")
        )

        # Convert numeric columns
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop malformed rows
        before = len(df)
        df = df.dropna(subset=["total_ms"])
        dropped = before - len(df)

        if dropped > 0:
            print(f"  [WARN] Dropped {dropped} malformed rows from {fname}")

        df["model_label"] = model_label

        frames.append(df)

        print(f"  Loaded {fname}: {len(df)} rows")

    if not frames:
        raise FileNotFoundError("No telemetry CSV files found in current directory.")

    combined = pd.concat(frames, ignore_index=True)

    # Derive resolution track
    combined["track"] = "C"
    combined.loc[combined["track_b_hit"], "track"] = "B"
    combined.loc[combined["track_a_hit"], "track"] = "A"

    return combined


# ── Section V-B  Cache Resolution Distribution ───────────────────────────────
def section_vb(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("SECTION V-B  —  Cache Resolution Distribution")
    print("=" * 60)

    total = len(df)
    print(
        f"\nTotal query-response pairs : {total}  (300 prompts × {df['model_label'].nunique()} runs)"
    )

    # Per-model breakdown
    per_model = (
        df.groupby(["model_label", "track"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=["A", "B", "C"])
    )
    per_model["Total"] = per_model.sum(axis=1)
    print(
        "\nPer-model resolution counts (each run is 300 prompts, cache cleared between runs):"
    )
    print(per_model.to_string())

    # Cumulative totals
    cum = df["track"].value_counts().reindex(["A", "B", "C"])
    print(f"\nCumulative totals across all runs:")
    for t in ["A", "B", "C"]:
        n = cum[t]
        pct = 100 * n / total
        print(f"  Track {t}: {n} queries  ({pct:.1f}%)")

    hit_rate = 100 * (cum["A"] + cum["B"]) / total
    print(f"\nCombined cache hit rate (A+B): {hit_rate:.1f}%")

    # Per-run hit rates — confirm model-agnostic behaviour
    print("\nPer-run hit rates (confirms model-agnostic reproducibility):")
    for model, grp in df.groupby("model_label"):
        n = len(grp)
        hits = (grp["track"] == "A").sum() + (grp["track"] == "B").sum()
        print(f"  {model}: {hits}/{n}  ({100 * hits / n:.1f}%)")


# ── Section V-C  Latency Reduction ───────────────────────────────────────────
def section_vc(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("SECTION V-C  —  Latency Reduction Analysis")
    print("=" * 60)

    stats = df.groupby("track")["total_ms"].agg(
        [
            "mean",
            "median",
            "std",
            lambda x: x.quantile(0.25),
            lambda x: x.quantile(0.75),
        ]
    )
    stats.columns = ["mean_ms", "median_ms", "std_ms", "q25_ms", "q75_ms"]
    stats = stats.reindex(["A", "B", "C"])
    print("\nLatency statistics by resolution track (ms):")
    print(stats.round(1).to_string())

    mean_c = stats.loc["C", "mean_ms"]
    for t in ["A", "B"]:
        mean_t = stats.loc[t, "mean_ms"]
        reduction = 100 * (mean_c - mean_t) / mean_c
        print(f"\n  Track {t} vs Track C: {reduction:.0f}% mean latency reduction")

    # IQR span check for box plot claim
    print("\nIQR spans (for log-scale box plot description):")
    for t in ["A", "B", "C"]:
        iqr = stats.loc[t, "q75_ms"] - stats.loc[t, "q25_ms"]
        print(f"  Track {t} IQR: {iqr:.1f} ms")


# ── Section V-D  Shannon AST vs Model AST ────────────────────────────────────
def section_vd(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("SECTION V-D  —  Shannon AST vs LLM-Driven Model AST")
    print("=" * 60)

    # Agreement: shannon_threshold == model_ast_threshold
    df = df.copy()
    df["ast_agree"] = df["shannon_threshold"] == df["model_ast_threshold"]

    print("\nPer-model agreement rate (Shannon AST vs Model AST):")
    agreement_rates = {}
    for model, grp in df.groupby("model_label"):
        rate = 100 * grp["ast_agree"].mean()
        agreement_rates[model] = rate
        print(f"  {model}: {rate:.1f}% agreement")

    rates = list(agreement_rates.values())
    spread = max(rates) - min(rates)
    print(f"\nRange across models: {min(rates):.1f}% – {max(rates):.1f}%")
    print(f"Max spread between models: {spread:.1f} percentage points")


# ── Section V-E  Semantic Trap Rejection ─────────────────────────────────────
def section_ve(df: pd.DataFrame, prompt_df: pd.DataFrame = None):
    print("\n" + "=" * 60)
    print("SECTION V-E  —  Semantic Trap Rejection Precision")
    print("=" * 60)

    # Identify trap queries via prompt dataset if available
    # Fallback: use high-entropy queries that went to Track B or C
    # Best approach: join on query text with the prompt dataset
    if prompt_df is not None and "category" in prompt_df.columns:
        trap_queries = set(
            prompt_df[prompt_df["category"] == "trap"]["query"].str.strip().str.lower()
        )
        df = df.copy()
        df["is_trap"] = df["query"].str.strip().str.lower().isin(trap_queries)
        trap_df = df[df["is_trap"]]
        print(
            f"\nTrap prompts identified via dataset.csv: {trap_df['query'].nunique()} unique"
        )
        print(f"Total trap evaluations (across all runs): {len(trap_df)}")
    else:
        # Fallback: cannot identify traps without the prompt dataset
        print(
            "\n  [INFO] dataset.csv not joined — trap analysis requires prompt categories."
        )
        print("         Place dataset.csv in the same directory and rerun.")
        return

    # True Negative = trap query correctly sent to Track C (not a false hit)
    # False Positive = trap query incorrectly served from cache (Track A or B)
    tn = (trap_df["track"] == "C").sum()
    fp = (trap_df["track"] != "C").sum()
    total_trap = len(trap_df)

    print(
        f"\nTrue Negatives  (correctly rejected → Track C): {tn}  ({100 * tn / total_trap:.1f}%)"
    )
    print(
        f"False Positives (incorrect cache hit):           {fp}  ({100 * fp / total_trap:.1f}%)"
    )

    # Per-model breakdown
    print("\nPer-model trap rejection:")
    for model, grp in trap_df.groupby("model_label"):
        tn_m = (grp["track"] == "C").sum()
        fp_m = (grp["track"] != "C").sum()
        n = len(grp)
        print(
            f"  {model}: TN={tn_m} ({100 * tn_m / n:.1f}%)  FP={fp_m} ({100 * fp_m / n:.1f}%)"
        )


# ── Table for paper: per-run resolution breakdown ────────────────────────────
def paper_table(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("PAPER TABLE  —  Per-Run Resolution Breakdown (paste into paper)")
    print("=" * 60)

    rows = []
    for model, grp in df.groupby("model_label"):
        a = (grp["track"] == "A").sum()
        b = (grp["track"] == "B").sum()
        c = (grp["track"] == "C").sum()
        rows.append(
            {
                "Model": model,
                "Track A": a,
                "Track B": b,
                "Track C": c,
                "Total": len(grp),
            }
        )

    # Totals row
    tdf = pd.DataFrame(rows)
    totals = tdf[["Track A", "Track B", "Track C", "Total"]].sum()
    totals["Model"] = "TOTAL"
    tdf = pd.concat([tdf, totals.to_frame().T], ignore_index=True)

    print()
    print(tdf.to_string(index=False))

    # Hit rate per model
    print("\nHit rates per model:")
    for _, row in tdf[tdf["Model"] != "TOTAL"].iterrows():
        hr = 100 * (row["Track A"] + row["Track B"]) / row["Total"]
        print(f"  {row['Model']}: {hr:.1f}%")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Loading telemetry files...")
    df = load_files()

    # Optionally load prompt dataset for trap identification
    prompt_df = None
    if Path("dataset.csv").exists():
        prompt_df = pd.read_csv("dataset.csv")
        print(f"  Loaded dataset.csv: {len(prompt_df)} prompts")

    section_vb(df)
    section_vc(df)
    section_vd(df)
    section_ve(df, prompt_df)
    paper_table(df)

    print("\n" + "=" * 60)
    print("Done. Copy the numbers above into the paper.")
    print("=" * 60)


if __name__ == "__main__":
    main()
