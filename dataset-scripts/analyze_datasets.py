#!/usr/bin/env python3
"""Analyze a CSV dataset for semantic-cache / query-routing experiments.

Works with the WildChat-style schema (Query, Candidate_Query, Exact_Hit,
Semantic_Hit, Guard_Checked, Guard_Blocked, Guard_Reason, latency columns,
thresholds, etc.) while tolerating optional/missing columns.

Examples
--------
python analyze_dataset.py wildchat_2k.csv
python analyze_dataset.py wildchat_2k.csv --out-dir analysis_output
python analyze_dataset.py data.csv --category-column Category
python analyze_dataset.py wildchat_2k.csv --auto-cluster 8

Optional automatic grouping requires scikit-learn. These clusters are rough,
unsupervised topic groups, not human-validated categories.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


# Normalize names to match e.g. "Guard_Blocked", "guard blocked", or "GUARD-BLOCKED".
def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def find_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    by_normalized = {_norm(c): c for c in df.columns}
    for candidate in candidates:
        found = by_normalized.get(_norm(candidate))
        if found is not None:
            return found
    return None


def parse_bool(value: Any) -> Any:
    """Parse common CSV booleans while preserving missing/unknown values."""
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, float, np.integer, np.floating)):
        if value == 1:
            return True
        if value == 0:
            return False
    text = str(value).strip().lower()
    if text in {"true", "t", "yes", "y", "1"}:
        return True
    if text in {"false", "f", "no", "n", "0"}:
        return False
    return pd.NA


def as_boolean(series: pd.Series) -> pd.Series:
    return series.map(parse_bool).astype("boolean")


def bool_metrics(series: pd.Series) -> dict[str, Any]:
    parsed = as_boolean(series)
    valid = int(parsed.notna().sum())
    positive = int(parsed.fillna(False).sum())
    return {
        "true_count": positive,
        "valid_count": valid,
        "true_rate_pct": round(100 * positive / valid, 3) if valid else None,
        "missing_or_unrecognized_count": int(parsed.isna().sum()),
    }


def numeric_series(df: pd.DataFrame, column: Optional[str]) -> Optional[pd.Series]:
    if column is None:
        return None
    result = pd.to_numeric(df[column], errors="coerce")
    return result if result.notna().any() else None


def numeric_stats(series: Optional[pd.Series]) -> Optional[dict[str, Any]]:
    if series is None:
        return None
    clean = series.dropna()
    if clean.empty:
        return None
    return {
        "count": int(clean.count()),
        "mean": round(float(clean.mean()), 4),
        "std": round(float(clean.std()), 4) if clean.count() > 1 else 0.0,
        "min": round(float(clean.min()), 4),
        "p50": round(float(clean.quantile(0.50)), 4),
        "p90": round(float(clean.quantile(0.90)), 4),
        "p95": round(float(clean.quantile(0.95)), 4),
        "p99": round(float(clean.quantile(0.99)), 4),
        "max": round(float(clean.max()), 4),
    }


def safe_filename(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(value)).strip("_")
    return value[:70] or "category"


def save_bar_chart(
    labels: list[str],
    values: list[float],
    title: str,
    y_label: str,
    output_path: Path,
    rotate: int = 25,
) -> None:
    if not labels:
        return
    width = max(7, min(16, 0.7 * len(labels) + 3))
    plt.figure(figsize=(width, 5))
    plt.bar(labels, values)
    plt.title(title)
    plt.ylabel(y_label)
    plt.xticks(rotation=rotate, ha="right")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def save_histogram(
    series: Optional[pd.Series],
    title: str,
    x_label: str,
    output_path: Path,
    bins: int = 35,
) -> None:
    if series is None:
        return
    clean = series.dropna()
    if clean.empty:
        return
    plt.figure(figsize=(8, 5))
    plt.hist(clean, bins=min(bins, max(5, int(np.sqrt(len(clean)) * 2))))
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel("Number of rows")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def maybe_cluster_queries(
    df: pd.DataFrame, query_col: str, n_clusters: int
) -> pd.Series:
    try:
        from sklearn.cluster import KMeans
        from sklearn.feature_extraction.text import TfidfVectorizer
    except ImportError as exc:
        raise RuntimeError(
            "--auto-cluster needs scikit-learn. Install it with: pip install scikit-learn"
        ) from exc

    texts = df[query_col].fillna("").astype(str).str.strip()
    unique_count = max(1, int(texts[texts != ""].nunique()))
    k = min(max(1, n_clusters), len(df), unique_count)
    if k < 2:
        return pd.Series(["AutoCluster_0"] * len(df), index=df.index, dtype="string")

    vectorizer = TfidfVectorizer(
        stop_words="english",
        ngram_range=(1, 2),
        max_features=12000,
        min_df=1,
        sublinear_tf=True,
    )
    matrix = vectorizer.fit_transform(texts.where(texts != "", "[empty query]"))
    labels = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(matrix)
    return pd.Series(
        [f"AutoCluster_{int(label)}" for label in labels],
        index=df.index,
        dtype="string",
    )


def analyze(
    input_path: Path,
    out_dir: Path,
    category_column: Optional[str],
    auto_cluster: int,
    top_n: int,
) -> dict[str, Any]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    # Low-memory disabled to avoid mixed-type warnings for wide text columns.
    df = pd.read_csv(input_path, low_memory=False)
    if df.empty:
        raise ValueError("The input CSV has headers but no data rows.")

    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir = out_dir / "charts"
    plots_dir.mkdir(parents=True, exist_ok=True)

    columns = {
        "timestamp": find_column(df, ["Timestamp", "Date", "Datetime", "Created_At"]),
        "query": find_column(
            df, ["Query", "User_Query", "Prompt", "Input_Text", "Text"]
        ),
        "candidate_query": find_column(
            df, ["Candidate_Query", "Candidate Query", "Matched_Query", "Cached_Query"]
        ),
        "model": find_column(df, ["Model", "Model_Name", "LLM"]),
        "exact_hit": find_column(df, ["Exact_Hit", "ExactHit", "Exact_Cache_Hit"]),
        "semantic_hit": find_column(
            df, ["Semantic_Hit", "SemanticHit", "Semantic_Cache_Hit"]
        ),
        "guard_checked": find_column(df, ["Guard_Checked", "GuardChecked"]),
        "guard_blocked": find_column(
            df, ["Guard_Blocked", "GuardBlocked", "Guard_Rejected"]
        ),
        "guard_reason": find_column(
            df, ["Guard_Reason", "GuardReason", "Block_Reason"]
        ),
        "distance": find_column(
            df, ["Distance_Score", "Distance", "Similarity_Score", "Score"]
        ),
        "trad_latency": find_column(
            df, ["Trad_Latency_Ms", "Traditional_Latency_Ms", "Baseline_Latency_Ms"]
        ),
        "sem_latency": find_column(
            df, ["Sem_Latency_Ms", "Semantic_Latency_Ms", "Cache_Lookup_Latency_Ms"]
        ),
        "total_latency": find_column(
            df, ["Total_Latency_Ms", "Latency_Ms", "End_To_End_Latency_Ms"]
        ),
        "shannon_thresh": find_column(df, ["Shannon_Thresh", "Shannon_Threshold"]),
        "model_thresh": find_column(df, ["Model_Thresh", "Model_Threshold"]),
    }

    cat_col = None
    if category_column:
        cat_col = find_column(df, [category_column])
        if cat_col is None:
            raise ValueError(
                f"Category column '{category_column}' was not found. Available columns: {', '.join(df.columns)}"
            )
    else:
        cat_col = find_column(
            df,
            [
                "Category",
                "Query_Category",
                "QueryCategory",
                "Topic",
                "Intent",
                "Domain",
                "Task_Type",
                "Query_Type",
                "Class",
                "Label",
                "Dataset_Category",
            ],
        )

    category_source = None
    if cat_col:
        df["_Analysis_Category"] = (
            df[cat_col].fillna("(missing category)").astype(str).str.strip()
        )
        df.loc[df["_Analysis_Category"] == "", "_Analysis_Category"] = (
            "(missing category)"
        )
        category_source = f"existing column: {cat_col}"
    elif auto_cluster > 0:
        query_col = columns["query"]
        if query_col is None:
            raise ValueError(
                "Cannot auto-cluster: no query/prompt/text column was detected."
            )
        df["_Analysis_Category"] = maybe_cluster_queries(df, query_col, auto_cluster)
        cat_col = "_Analysis_Category"
        category_source = f"unsupervised TF-IDF/KMeans clusters based on {query_col}"

    # Parse booleans and numeric columns into analysis-friendly series.
    bool_columns = {
        key: columns[key]
        for key in ["exact_hit", "semantic_hit", "guard_checked", "guard_blocked"]
        if columns[key]
    }
    bool_data = {key: as_boolean(df[col]) for key, col in bool_columns.items()}
    nums = {
        key: numeric_series(df, columns[key])
        for key in [
            "distance",
            "trad_latency",
            "sem_latency",
            "total_latency",
            "shannon_thresh",
            "model_thresh",
        ]
    }

    n_rows = int(len(df))
    metrics: dict[str, Any] = {
        "input_file": str(input_path.resolve()),
        "rows": n_rows,
        "columns_count": int(
            len(df.columns) - (1 if "_Analysis_Category" in df.columns else 0)
        ),
        "column_names": [str(c) for c in df.columns if c != "_Analysis_Category"],
        "detected_columns": {
            key: value for key, value in columns.items() if value is not None
        },
        "missing_values_by_column": {
            str(k): int(v)
            for k, v in df.drop(columns=["_Analysis_Category"], errors="ignore")
            .isna()
            .sum()
            .items()
        },
        "category_analysis": {
            "available": category_source is not None,
            "source": category_source,
            "distinct_categories": int(df["_Analysis_Category"].nunique())
            if category_source
            else 0,
        },
    }

    query_col = columns["query"]
    if query_col:
        query_text = df[query_col].fillna("").astype(str).str.strip()
        metrics["query_stats"] = {
            "unique_nonempty_queries": int(query_text[query_text != ""].nunique()),
            "duplicate_query_rows_after_first": int(
                query_text[query_text != ""].duplicated().sum()
            ),
            "empty_query_rows": int((query_text == "").sum()),
            "average_query_characters": round(float(query_text.str.len().mean()), 2),
            "median_query_characters": round(float(query_text.str.len().median()), 2),
        }

    timestamp_col = columns["timestamp"]
    if timestamp_col:
        times = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True).dropna()
        metrics["time_range"] = {
            "valid_timestamps": int(len(times)),
            "start_utc": times.min().isoformat() if len(times) else None,
            "end_utc": times.max().isoformat() if len(times) else None,
        }

    if columns["model"]:
        counts = (
            df[columns["model"]]
            .fillna("(missing)")
            .astype(str)
            .value_counts(dropna=False)
        )
        metrics["model_counts"] = {str(k): int(v) for k, v in counts.items()}

    metrics["boolean_metrics"] = {}
    for key, col in bool_columns.items():
        metrics["boolean_metrics"][key] = bool_metrics(df[col])

    metrics["numeric_metrics"] = {}
    for key, series in nums.items():
        if series is not None:
            metrics["numeric_metrics"][key] = numeric_stats(series)

    # Joint hit/guard outcomes; missing boolean values are shown as Unknown, not silently treated as False.
    if "exact_hit" in bool_data or "semantic_hit" in bool_data:
        exact = bool_data.get(
            "exact_hit", pd.Series(pd.NA, index=df.index, dtype="boolean")
        )
        semantic = bool_data.get(
            "semantic_hit", pd.Series(pd.NA, index=df.index, dtype="boolean")
        )
        combo = pd.DataFrame(
            {
                "Exact_Hit": exact.map({True: "True", False: "False"}).fillna(
                    "Unknown"
                ),
                "Semantic_Hit": semantic.map({True: "True", False: "False"}).fillna(
                    "Unknown"
                ),
            }
        )
        combo_counts = combo.value_counts().rename("count").reset_index()
        combo_counts.to_csv(out_dir / "hit_combinations.csv", index=False)
        metrics["hit_combinations"] = [
            {
                "exact_hit": row["Exact_Hit"],
                "semantic_hit": row["Semantic_Hit"],
                "count": int(row["count"]),
            }
            for _, row in combo_counts.iterrows()
        ]

    guard_reason_col = columns["guard_reason"]
    if guard_reason_col:
        reason = (
            df[guard_reason_col]
            .fillna("(no reason / not blocked)")
            .astype(str)
            .str.strip()
        )
        reason.loc[reason == ""] = "(empty reason)"
        reason_counts = (
            reason.value_counts().rename_axis("Guard_Reason").reset_index(name="Count")
        )
        reason_counts.to_csv(out_dir / "guard_reason_counts.csv", index=False)
        metrics["guard_reason_counts"] = {
            str(r["Guard_Reason"]): int(r["Count"]) for _, r in reason_counts.iterrows()
        }

    # Per-category summary uses valid denominators, so missing labels do not inflate hit rates.
    if category_source:
        category_rows: list[dict[str, Any]] = []
        working = df.copy()
        for category, group in working.groupby("_Analysis_Category", dropna=False):
            row: dict[str, Any] = {"Category": str(category), "Rows": int(len(group))}
            for metric_key, nice_name in [
                ("exact_hit", "Exact_Hit"),
                ("semantic_hit", "Semantic_Hit"),
                ("guard_checked", "Guard_Checked"),
                ("guard_blocked", "Guard_Blocked"),
            ]:
                col = columns.get(metric_key)
                if col:
                    b = as_boolean(group[col])
                    denominator = int(b.notna().sum())
                    positives = int(b.fillna(False).sum())
                    row[f"{nice_name}_Count"] = positives
                    row[f"{nice_name}_Rate_Pct"] = (
                        round(100 * positives / denominator, 3)
                        if denominator
                        else np.nan
                    )
            for metric_key, nice_name in [
                ("distance", "Distance_Score"),
                ("trad_latency", "Trad_Latency_Ms"),
                ("sem_latency", "Sem_Latency_Ms"),
                ("total_latency", "Total_Latency_Ms"),
            ]:
                col = columns.get(metric_key)
                if col:
                    val = pd.to_numeric(group[col], errors="coerce")
                    if val.notna().any():
                        row[f"{nice_name}_Mean"] = round(float(val.mean()), 4)
                        row[f"{nice_name}_Median"] = round(float(val.median()), 4)
            category_rows.append(row)
        category_summary = pd.DataFrame(category_rows)
        sort_column = "Rows"
        category_summary.sort_values(sort_column, ascending=False).to_csv(
            out_dir / "category_summary.csv", index=False
        )
        metrics["category_summary_file"] = "category_summary.csv"

        # Category size and selected outcome charts.
        top_categories = category_summary.sort_values("Rows", ascending=False).head(
            top_n
        )
        save_bar_chart(
            top_categories["Category"].astype(str).tolist(),
            top_categories["Rows"].astype(float).tolist(),
            f"Top {len(top_categories)} Categories by Row Count",
            "Rows",
            plots_dir / "category_counts.png",
        )
        for rate_column, title, filename in [
            (
                "Semantic_Hit_Rate_Pct",
                "Semantic Hit Rate by Category",
                "semantic_hit_rate_by_category.png",
            ),
            (
                "Exact_Hit_Rate_Pct",
                "Exact Hit Rate by Category",
                "exact_hit_rate_by_category.png",
            ),
            (
                "Guard_Blocked_Rate_Pct",
                "Guard Block Rate by Category",
                "guard_block_rate_by_category.png",
            ),
        ]:
            if rate_column in category_summary.columns:
                plot_data = (
                    category_summary.dropna(subset=[rate_column])
                    .sort_values("Rows", ascending=False)
                    .head(top_n)
                )
                save_bar_chart(
                    plot_data["Category"].astype(str).tolist(),
                    plot_data[rate_column].astype(float).tolist(),
                    title,
                    "Rate (%)",
                    plots_dir / filename,
                )

    # Generic charts.
    hit_labels, hit_values = [], []
    for key, label in [
        ("exact_hit", "Exact hit"),
        ("semantic_hit", "Semantic hit"),
        ("guard_blocked", "Guard blocked"),
    ]:
        if key in bool_data:
            b = bool_data[key]
            denom = int(b.notna().sum())
            if denom:
                hit_labels.append(label)
                hit_values.append(float(100 * b.fillna(False).sum() / denom))
    if hit_labels:
        save_bar_chart(
            hit_labels,
            hit_values,
            "Hit / Guard Rates",
            "Rate (%)",
            plots_dir / "hit_guard_rates.png",
            rotate=0,
        )

    for key, title, xlabel, filename in [
        (
            "distance",
            "Distance Score Distribution",
            "Distance score",
            "distance_score_distribution.png",
        ),
        (
            "trad_latency",
            "Traditional / Baseline Latency Distribution",
            "Latency (ms)",
            "traditional_latency_distribution.png",
        ),
        (
            "sem_latency",
            "Semantic Lookup Latency Distribution",
            "Latency (ms)",
            "semantic_latency_distribution.png",
        ),
        (
            "total_latency",
            "Total End-to-End Latency Distribution",
            "Latency (ms)",
            "total_latency_distribution.png",
        ),
        (
            "shannon_thresh",
            "Shannon Threshold Distribution",
            "Threshold",
            "shannon_threshold_distribution.png",
        ),
        (
            "model_thresh",
            "Model Threshold Distribution",
            "Threshold",
            "model_threshold_distribution.png",
        ),
    ]:
        save_histogram(nums.get(key), title, xlabel, plots_dir / filename)

    if guard_reason_col:
        reason = (
            df[guard_reason_col]
            .fillna("(no reason / not blocked)")
            .astype(str)
            .str.strip()
        )
        reason.loc[reason == ""] = "(empty reason)"
        reason_counts = reason.value_counts().head(top_n)
        save_bar_chart(
            reason_counts.index.astype(str).tolist(),
            reason_counts.values.astype(float).tolist(),
            "Guard Reasons (Top Values)",
            "Rows",
            plots_dir / "guard_reasons.png",
        )

    # Write a machine-readable summary, a flattened key/value CSV, and readable report.
    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False, default=str)

    flat_metrics: list[dict[str, Any]] = [
        {"Metric": "rows", "Value": n_rows},
        {"Metric": "columns_count", "Value": metrics["columns_count"]},
    ]
    for key, values in metrics.get("boolean_metrics", {}).items():
        for subkey, value in values.items():
            flat_metrics.append({"Metric": f"{key}.{subkey}", "Value": value})
    for key, values in metrics.get("numeric_metrics", {}).items():
        for subkey, value in values.items():
            flat_metrics.append({"Metric": f"{key}.{subkey}", "Value": value})
    if "query_stats" in metrics:
        for key, value in metrics["query_stats"].items():
            flat_metrics.append({"Metric": f"query_stats.{key}", "Value": value})
    pd.DataFrame(flat_metrics).to_csv(out_dir / "metrics.csv", index=False)

    report_lines = [
        "DATASET ANALYSIS REPORT",
        "=" * 72,
        f"Input: {input_path.resolve()}",
        f"Rows: {n_rows:,}",
        f"Columns: {metrics['columns_count']}",
        "",
        "DETECTED COLUMNS",
        "-" * 72,
    ]
    report_lines.extend(
        [f"{key}: {value}" for key, value in metrics["detected_columns"].items()]
    )
    report_lines.extend(["", "BOOLEAN METRICS", "-" * 72])
    for key, values in metrics["boolean_metrics"].items():
        pct = values["true_rate_pct"]
        rate_text = f"{pct:.3f}%" if pct is not None else "N/A"
        report_lines.append(
            f"{key}: {values['true_count']:,}/{values['valid_count']:,} true ({rate_text}); missing/unrecognized={values['missing_or_unrecognized_count']:,}"
        )
    report_lines.extend(["", "NUMERIC METRICS", "-" * 72])
    for key, values in metrics["numeric_metrics"].items():
        report_lines.append(
            f"{key}: count={values['count']:,}, mean={values['mean']}, median={values['p50']}, "
            f"p90={values['p90']}, p95={values['p95']}, max={values['max']}"
        )
    if "query_stats" in metrics:
        report_lines.extend(["", "QUERY STATS", "-" * 72])
        for key, value in metrics["query_stats"].items():
            report_lines.append(f"{key}: {value}")
    if "time_range" in metrics:
        report_lines.extend(["", "TIME RANGE", "-" * 72])
        report_lines.append(
            f"Valid timestamps: {metrics['time_range']['valid_timestamps']:,}"
        )
        report_lines.append(f"Start (UTC): {metrics['time_range']['start_utc']}")
        report_lines.append(f"End (UTC): {metrics['time_range']['end_utc']}")
    report_lines.extend(["", "CATEGORY ANALYSIS", "-" * 72])
    if category_source:
        report_lines.append(f"Category source: {category_source}")
        report_lines.append(
            f"Distinct categories: {metrics['category_analysis']['distinct_categories']}"
        )
        report_lines.append("See category_summary.csv for per-category metrics.")
        if auto_cluster > 0 and category_source.startswith("unsupervised"):
            report_lines.append(
                "IMPORTANT: AutoCluster labels are inferred groups, not verified semantic categories."
            )
    else:
        report_lines.append(
            "No category column found. Overall analysis was completed without category breakdowns."
        )
        report_lines.append(
            "Pass --category-column COLUMN_NAME when a label column is available, or --auto-cluster N for rough query clusters."
        )
    if "guard_reason_counts" in metrics:
        report_lines.extend(["", "GUARD REASONS", "-" * 72])
        for reason, count in metrics["guard_reason_counts"].items():
            report_lines.append(f"{reason}: {count:,}")
    report_lines.extend(["", "OUTPUTS", "-" * 72])
    report_lines.append("summary.json: complete structured summary")
    report_lines.append("metrics.csv: flattened metric values")
    report_lines.append(
        "hit_combinations.csv: Exact_Hit / Semantic_Hit combinations, when available"
    )
    report_lines.append("guard_reason_counts.csv: guard reason counts, when available")
    report_lines.append(
        "category_summary.csv: per-category metrics, when category analysis is available"
    )
    report_lines.append("charts/: generated PNG visualizations")

    report_path = out_dir / "report.txt"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze a CSV containing queries, cache hits, guard results, scores, and latency measurements."
    )
    parser.add_argument("csv_file", type=Path, help="Input CSV file")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("analysis_output"),
        help="Directory for the report, CSV summaries, JSON, and charts (default: analysis_output)",
    )
    parser.add_argument(
        "--category-column",
        type=str,
        default=None,
        help="Optional category/label column to analyze (auto-detected when omitted)",
    )
    parser.add_argument(
        "--auto-cluster",
        type=int,
        default=0,
        metavar="N",
        help="If no category column exists, infer up to N rough query clusters (requires scikit-learn; default: off)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Maximum categories/reasons shown in charts (default: 20)",
    )
    args = parser.parse_args()

    if args.auto_cluster < 0:
        parser.error("--auto-cluster must be 0 or a positive integer")
    if args.top_n < 1:
        parser.error("--top-n must be at least 1")

    try:
        metrics = analyze(
            args.csv_file,
            args.out_dir,
            args.category_column,
            args.auto_cluster,
            args.top_n,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Analysis complete: {args.out_dir.resolve()}")
    print(f"Rows analyzed: {metrics['rows']:,}")
    if metrics["category_analysis"]["available"]:
        print(
            f"Category analysis: {metrics['category_analysis']['distinct_categories']} categories ({metrics['category_analysis']['source']})"
        )
    else:
        print(
            "Category analysis: no category column detected; overall metrics generated."
        )
    print(f"Main report: {(args.out_dir / 'report.txt').resolve()}")
    print(f"Charts: {(args.out_dir / 'charts').resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
