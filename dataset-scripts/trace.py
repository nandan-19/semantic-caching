#!/usr/bin/env python3
"""
Score a benchmark run against ground-truth intents.

  python evaluate_trace.py --trace trace.csv --results research_results.csv

Delete research_results.csv (and reset the Redis cache) before each run: the exporter appends,
and the trace must map 1:1, in order, onto the rows.
"""

import argparse
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--trace", default="trace.csv")
ap.add_argument("--results", default="research_results.csv")
a = ap.parse_args()

tr = pd.read_csv(a.trace)
if "intent" not in tr:  # 2-column files from fetch_datasets.py: query,category
    tr["intent"] = tr["category"]
if "phase" not in tr:
    tr["phase"] = "all"
labeled = tr["intent"].nunique() > 1
intent_of = dict(zip(tr["query"], tr["intent"]))
r = pd.read_csv(a.results)
for c in ["Exact_Hit", "Semantic_Hit", "Guard_Checked", "Guard_Blocked"]:
    r[c] = r[c].astype(str).str.lower().eq("true")

if not labeled:
    n = len(r)
    sem = r["Semantic_Hit"]
    print(
        f"\nNo ground-truth labels in this file (category is constant).\n"
        f"rows={n}  exact={100 * r['Exact_Hit'].mean():.1f}%  semantic={100 * sem.mean():.1f}%  "
        f"miss={100 * (~(r['Exact_Hit'] | sem)).mean():.1f}%  avg_latency_ms={r['Total_Latency_Ms'].mean():.1f}"
    )
    r[sem].sample(min(100, int(sem.sum())), random_state=1)[
        ["Query", "Candidate_Query", "Distance_Score"]
    ].assign(correct="").to_csv("semantic_hits_to_review.csv", index=False)
    print(
        "Wrote semantic_hits_to_review.csv - fill the `correct` column (1/0) for ~100 hits to get precision."
    )
    raise SystemExit

if len(r) == len(tr):
    r["phase"] = tr["phase"].values
else:
    print(
        f"WARNING: {len(r)} result rows vs {len(tr)} trace rows -> no per-phase breakdown"
    )
    r["phase"] = "all"

r["q_intent"] = r["Query"].map(intent_of)
r["c_intent"] = r["Candidate_Query"].map(intent_of)
r["same"] = r["q_intent"] == r["c_intent"]
# cacheable = an equivalent query (same intent) was already stored earlier in this run
r["cacheable"] = r.groupby("q_intent").cumcount() > 0
hit = r["Exact_Hit"] | r["Semantic_Hit"]
r["good_sem"] = r["Semantic_Hit"] & r["same"]
r["bad_sem"] = r["Semantic_Hit"] & ~r["same"]


def summarize(g):
    n = len(g)
    h = (g["Exact_Hit"] | g["Semantic_Hit"]).sum()
    cacheable = g["cacheable"].sum()
    return pd.Series(
        {
            "n": n,
            "exact%": 100 * g["Exact_Hit"].mean(),
            "sem_ok%": 100 * g["good_sem"].mean(),
            "sem_false%": 100 * g["bad_sem"].mean(),
            "miss%": 100 * (n - h) / n,
            "hit_precision%": 100
            * (g["Exact_Hit"].sum() + g["good_sem"].sum())
            / max(h, 1),
            "recall_of_cacheable%": 100
            * (g["Exact_Hit"].sum() + g["good_sem"].sum())
            / max(cacheable, 1),
            "avg_latency_ms": g["Total_Latency_Ms"].mean(),
        }
    )


print("\n== Outcomes by phase (actual run: Shannon AST + guard) ==")
print(
    pd.concat(
        [
            r.groupby("phase").apply(summarize, include_groups=False),
            summarize(r).to_frame("ALL").T,
        ]
    )
    .round(1)
    .to_string()
)

gc = r[r["Guard_Checked"]]
print("\n== Semantic guard ==")
print(f"checked: {len(gc)}  blocked: {gc['Guard_Blocked'].sum()}")
print(
    f"  correct blocks (different intent): {(gc['Guard_Blocked'] & ~gc['same']).sum()}"
)
print(
    f"  over-blocks (same intent, hit lost): {(gc['Guard_Blocked'] & gc['same']).sum()}"
)
print(f"  let through but wrong (false hits): {r['bad_sem'].sum()}")
if r["bad_sem"].any():
    print("  sample false-hit pairs:")
    print(
        r[r["bad_sem"]][["Query", "Candidate_Query", "Distance_Score"]]
        .head(8)
        .to_string(index=False)
    )

print("\n== Threshold what-if on Track-A misses (nearest neighbour only, guard OFF) ==")
m = r[~r["Exact_Hit"] & r["c_intent"].notna()]
rows = []
for name, thr in [
    ("fixed 0.15", 0.15),
    ("fixed 0.20", 0.20),
    ("fixed 0.25", 0.25),
    ("fixed 0.35", 0.35),
    ("shannon AST", m["Shannon_Thresh"]),
]:
    h = m["Distance_Score"] <= thr
    ok, bad = (h & m["same"]).sum(), (h & ~m["same"]).sum()
    rows.append((name, ok, bad, 100 * ok / max(ok + bad, 1)))
print(
    pd.DataFrame(rows, columns=["policy", "correct_hits", "false_hits", "precision%"])
    .round(1)
    .to_string(index=False)
)
print("(valid as a comparison on one run; cache state is the run's own)")
