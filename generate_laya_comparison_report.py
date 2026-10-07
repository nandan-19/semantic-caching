from pathlib import Path
import csv
import statistics
import textwrap

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Image, PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parent
BASELINE = ROOT / "data" / "qwen.csv"
LAYA = ROOT / "results" / "laya_cache_audit_v2.csv"
OUTPUT_DIR = ROOT / "docs"
OUTPUT_PDF = OUTPUT_DIR / "laya_comparative_performance_report.pdf"
ASSET_DIR = OUTPUT_DIR / "laya_report_assets"
ASSET_DIR.mkdir(parents=True, exist_ok=True)


def as_bool(value):
    return str(value).strip().lower() == "true"


def load_csv(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def number(rows, field):
    return [float(row[field]) for row in rows if row.get(field) not in (None, "")]


def mean(values):
    return statistics.mean(values) if values else 0.0


def median(values):
    return statistics.median(values) if values else 0.0


def pct(value, total):
    return 100.0 * value / total if total else 0.0


def baseline_stats(rows):
    exact = [r for r in rows if as_bool(r["Exact_Hit"])]
    semantic = [r for r in rows if as_bool(r["Semantic_Hit"])]
    misses = [r for r in rows if not as_bool(r["Exact_Hit"]) and not as_bool(r["Semantic_Hit"])]
    return {
        "n": len(rows), "exact": len(exact), "semantic": len(semantic), "miss": len(misses),
        "hit_rate": pct(len(exact) + len(semantic), len(rows)),
        "all_latency": mean(number(rows, "Total_Latency_Ms")),
        "exact_latency": mean(number(exact, "Total_Latency_Ms")),
        "semantic_latency": mean(number(semantic, "Total_Latency_Ms")),
        "miss_latency": mean(number(misses, "Total_Latency_Ms")),
        "p50": median(number(rows, "Total_Latency_Ms")),
    }


def laya_stats(rows):
    exact = [r for r in rows if as_bool(r["exact_match_hit"])]
    semantic = [r for r in rows if as_bool(r["semantic_match_hit"])]
    misses = [r for r in rows if not as_bool(r["exact_match_hit"]) and not as_bool(r["semantic_match_hit"])]
    called = [r for r in rows if as_bool(r["laya_called"])]
    approved = [r for r in called if r["laya_decision"] == "laya_approved"]
    rejected = [r for r in called if r["laya_decision"] == "laya_rejected"]
    shannon = [r for r in rows if as_bool(r["shannon_accepted"])]
    candidate = [r for r in rows if as_bool(r["candidate_eligible"])]
    return {
        "n": len(rows), "exact": len(exact), "semantic": len(semantic), "miss": len(misses),
        "hit_rate": pct(len(exact) + len(semantic), len(rows)),
        "all_latency": mean(number(rows, "total_latency_ms")),
        "exact_latency": mean(number(exact, "total_latency_ms")),
        "semantic_latency": mean(number(semantic, "total_latency_ms")),
        "miss_latency": mean(number(misses, "total_latency_ms")),
        "p50": median(number(rows, "total_latency_ms")),
        "called": len(called), "approved": len(approved), "rejected": len(rejected),
        "approval_rate": pct(len(approved), len(called)),
        "laya_latency": mean(number(called, "laya_latency_ms")),
        "called_latency": mean(number(called, "total_latency_ms")),
        "approved_latency": mean(number(approved, "total_latency_ms")),
        "rejected_latency": mean(number(rejected, "total_latency_ms")),
        "shannon": len(shannon), "candidate": len(candidate),
        "recovery_candidates": len([r for r in candidate if not as_bool(r["shannon_accepted"])]),
        "probabilities": number(called, "laya_probability"),
    }


def save_comparison_chart(base, laya):
    labels = ["Exact hit", "Semantic hit", "Fallback miss"]
    base_values = [base["exact"], base["semantic"], base["miss"]]
    laya_values = [laya["exact"], laya["semantic"], laya["miss"]]
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    x = range(len(labels))
    width = 0.36
    ax.bar([i - width / 2 for i in x], base_values, width, label="Without Laya", color="#3973ac")
    ax.bar([i + width / 2 for i in x], laya_values, width, label="With Laya", color="#d97b29")
    ax.set_title("Same-Dataset Resolution Distribution")
    ax.set_ylabel("Requests")
    ax.set_xticks(list(x), labels)
    ax.legend()
    ax.grid(axis="y", alpha=0.25)
    for positions, values in (([i - width / 2 for i in x], base_values), ([i + width / 2 for i in x], laya_values)):
        for pos, value in zip(positions, values):
            ax.text(pos, value + 2, str(value), ha="center", fontsize=9)
    fig.tight_layout()
    path = ASSET_DIR / "resolution_comparison.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def save_latency_chart(base, laya):
    labels = ["All", "Exact", "Semantic", "Fallback"]
    base_values = [base["all_latency"], base["exact_latency"], base["semantic_latency"], base["miss_latency"]]
    laya_values = [laya["all_latency"], laya["exact_latency"], laya["semantic_latency"], laya["miss_latency"]]
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    x = range(len(labels))
    width = 0.36
    ax.bar([i - width / 2 for i in x], base_values, width, label="Without Laya", color="#3973ac")
    ax.bar([i + width / 2 for i in x], laya_values, width, label="With Laya", color="#d97b29")
    ax.set_title("Mean Total Latency by Resolution Path")
    ax.set_ylabel("Milliseconds")
    ax.set_xticks(list(x), labels)
    ax.legend()
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    path = ASSET_DIR / "latency_comparison.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def save_laya_diagnostics(rows):
    called = [r for r in rows if as_bool(r["laya_called"])]
    probabilities = number(called, "laya_probability")
    distances = number(called, "distance_score")
    decisions = [r["laya_decision"] for r in called]
    fig, axes = plt.subplots(1, 2, figsize=(9, 4))
    axes[0].hist(probabilities, bins=10, color="#6a4c93", edgecolor="white")
    axes[0].axvline(0.8, color="#d62828", linestyle="--", label="0.80 threshold")
    axes[0].set_title("Laya Probability Distribution")
    axes[0].set_xlabel("P(safe to return)")
    axes[0].set_ylabel("Calls")
    axes[0].legend(fontsize=8)
    approved = [d == "laya_approved" for d in decisions]
    colors_by_decision = ["#2a9d8f" if value else "#e76f51" for value in approved]
    axes[1].scatter(distances, probabilities, c=colors_by_decision, alpha=0.75, edgecolors="white")
    axes[1].axhline(0.8, color="#d62828", linestyle="--")
    axes[1].axvline(0.35, color="#264653", linestyle="--", label="candidate band")
    axes[1].set_title("Distance vs Laya Decision")
    axes[1].set_xlabel("Cosine distance")
    axes[1].set_ylabel("Laya probability")
    axes[1].legend(fontsize=8)
    fig.tight_layout()
    path = ASSET_DIR / "laya_diagnostics.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


def save_architecture_diagram():
    fig, ax = plt.subplots(figsize=(12, 7.2))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 8)
    ax.axis("off")
    def panel(x, y, w, h, title, color):
        ax.add_patch(FancyBboxPatch(
            (x, y), w, h, boxstyle="round,pad=0.08", facecolor=color,
            edgecolor="#4a4a4a", linewidth=1.2,
        ))
        ax.text(x + 0.16, y + h - 0.18, title, ha="left", va="top", fontsize=9, weight="bold")

    def node(x, y, w, h, label, color):
        ax.add_patch(FancyBboxPatch(
            (x, y), w, h, boxstyle="round,pad=0.04", facecolor=color,
            edgecolor="#333333", linewidth=1,
        ))
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center", fontsize=8.5)

    def arrow(start, end, label, label_xy=None, color="#3f3f3f", style="-"):
        ax.add_patch(FancyArrowPatch(
            start, end, arrowstyle="->", mutation_scale=12, linewidth=1.2,
            linestyle=style, color=color,
        ))
        if label:
            x, y = label_xy or ((start[0] + end[0]) / 2, (start[1] + end[1]) / 2)
            ax.text(x, y, label, fontsize=7.5, ha="center", va="center", color=color,
                    bbox=dict(facecolor="white", edgecolor="none", pad=1.2))

    panel(0.25, 5.8, 1.35, 1.2, "CLIENT", "#e4f1fb")
    node(0.48, 6.15, 0.9, 0.45, "gRPC\nrequest", "#b9dcf3")

    panel(1.95, 3.05, 5.15, 4.35, "GO GATEWAY - gRPC INTERCEPTOR / DECISION PLANE", "#fff2c6")
    node(2.25, 6.15, 1.25, 0.55, "1. Exact\nRedis lookup", "#f7d774")
    node(3.8, 6.15, 1.45, 0.55, "2. Embed +\nHNSW search", "#f7d774")
    node(5.55, 6.15, 1.25, 0.55, "3. Shannon\npolicy", "#f7d774")
    node(3.0, 4.75, 1.85, 0.65, "4. Candidate\neligible?", "#f4c6c6")
    node(5.35, 4.75, 1.45, 0.65, "5. Laya\nverify", "#d8c4e8")
    node(2.55, 3.55, 1.65, 0.65, "Approved:\nreturn cache", "#bde3c0")
    node(5.1, 3.55, 1.65, 0.65, "Rejected:\nfallback", "#f2c2bd")

    panel(7.55, 5.25, 2.05, 1.75, "DATA LAYER", "#f8dddd")
    node(7.82, 5.68, 1.5, 0.7, "Redis Hashes\nExact + HNSW", "#f4a6a6")

    panel(7.55, 3.0, 2.05, 1.55, "MODEL SERVICES", "#e0effa")
    node(7.78, 3.38, 1.55, 0.7, "Python Encoder\nMiniLM 384D", "#bde0fe")

    panel(10.0, 5.25, 1.7, 1.75, "FALLBACK", "#dff1df")
    node(10.25, 5.68, 1.2, 0.7, "Ollama\nGeneration", "#b7e4c7")

    panel(10.0, 3.0, 1.7, 1.55, "OBSERVABILITY", "#fff0c2")
    node(10.27, 3.38, 1.15, 0.7, "Kafka\nTelemetry", "#e9c46a")

    arrow((1.6, 6.4), (2.25, 6.4), "request", (1.92, 6.58))
    arrow((3.5, 6.42), (3.8, 6.42), "miss", (3.65, 6.65))
    arrow((5.25, 6.42), (5.55, 6.42), "distance", (5.4, 6.65))
    arrow((4.5, 6.15), (4.0, 5.4), "candidate", (4.55, 5.72))
    arrow((6.15, 6.15), (6.05, 5.4), "threshold", (6.55, 5.75))
    arrow((4.85, 5.08), (5.35, 5.08), "eligible", (5.1, 5.32))
    arrow((3.85, 4.75), (3.4, 4.2), "yes", (3.45, 4.5))
    arrow((6.05, 4.75), (5.9, 4.2), "approve", (6.45, 4.5), color="#2f7d32")
    arrow((5.35, 4.05), (5.85, 4.2), "reject", (5.45, 4.28), color="#b23b31")
    arrow((2.55, 3.88), (1.6, 6.1), "response", (1.95, 4.85), color="#2f7d32")
    arrow((6.75, 3.88), (10.25, 5.68), "generate", (8.55, 4.6), color="#b23b31")
    arrow((4.45, 6.15), (8.55, 5.68), "HNSW query", (6.75, 5.92))
    arrow((4.45, 3.55), (8.55, 5.68), "promote phrasing", (6.2, 4.05), color="#2f7d32", style="--")
    arrow((3.0, 3.2), (10.8, 3.38), "async event log", (8.0, 3.15), color="#8a6d1d", style="--")
    arrow((8.55, 3.38), (6.8, 4.75), "embedding", (7.6, 4.22), color="#26709b")
    ax.text(6.0, 1.55, "Decision order: exact lookup -> candidate retrieval -> Shannon/candidate policy -> Laya verification -> cached response or fallback generation", ha="center", fontsize=9, weight="bold")
    ax.text(6.0, 1.1, "Laya does not retrieve candidates; it evaluates only the nearest cached query-response pair.", ha="center", fontsize=8, color="#555555")
    fig.tight_layout()
    path = ASSET_DIR / "current_architecture.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return path


def html(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br/>")


def paragraph(text, style):
    return Paragraph(text, style)


def build_report():
    baseline_rows = load_csv(BASELINE)
    laya_rows = load_csv(LAYA)
    base = baseline_stats(baseline_rows)
    laya = laya_stats(laya_rows)
    resolution_chart = save_comparison_chart(base, laya)
    latency_chart = save_latency_chart(base, laya)
    diagnostics_chart = save_laya_diagnostics(laya_rows)
    architecture = save_architecture_diagram()

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="ReportTitle", parent=styles["Title"], alignment=TA_CENTER, fontSize=20, leading=25, spaceAfter=12))
    styles.add(ParagraphStyle(name="ReportH1", parent=styles["Heading1"], fontSize=15, leading=18, spaceBefore=12, spaceAfter=7, textColor=colors.HexColor("#17324d")))
    styles.add(ParagraphStyle(name="ReportH2", parent=styles["Heading2"], fontSize=11.5, leading=14, spaceBefore=9, spaceAfter=5, textColor=colors.HexColor("#245b7a")))
    styles.add(ParagraphStyle(name="ReportBody", parent=styles["BodyText"], fontSize=9.1, leading=12.2, spaceAfter=6))
    styles.add(ParagraphStyle(name="ReportSmall", parent=styles["BodyText"], fontSize=7.8, leading=9.6, spaceAfter=4))
    styles.add(ParagraphStyle(name="ReportCaption", parent=styles["BodyText"], fontSize=7.8, leading=9.5, alignment=TA_CENTER, textColor=colors.grey, spaceAfter=7))

    story = []
    story.append(paragraph("Comparative Performance Report: Semantic Cache With and Without Laya", styles["ReportTitle"]))
    story.append(paragraph("FSCgRPC | Same-dataset comparison | Generated 2026-10-07", styles["ReportSmall"]))
    story.append(Spacer(1, 6))
    story.append(paragraph("Executive conclusion", styles["ReportH1"]))
    story.append(paragraph("The Laya extension adds a useful semantic verification stage and produces better evidence about why a candidate was accepted or rejected. In the supplied 300-request run, however, it does not yet demonstrate an overall performance improvement. Compared with the old qwen baseline, the Laya run has fewer semantic hits, a lower combined cache-hit rate, and substantially higher latency for rejected candidates. Laya prevents several unsafe substitutions, but it also rejects many valid paraphrases and still approves critical entity substitutions. The next engineering step should be token-consistency checking and calibration, not simply lowering the Laya threshold.", styles["ReportBody"]))

    story.append(paragraph("Scope and comparability", styles["ReportH1"]))
    story.append(paragraph("This report compares data/qwen.csv, the older implementation's qwen run, with results/laya_cache_audit_v2.csv, the new Laya audit. Both are treated as 300-request runs because the supplied files contain 300 rows. The comparison is useful but should be interpreted as an operational benchmark, not a perfectly controlled causal experiment: the runs occurred at different times, model/cache state may differ, and the Laya audit contains richer path fields than the old CSV. The 901-query paper results are reported separately as historical context and are not merged into the 300-request comparison.", styles["ReportBody"]))

    story.append(paragraph("1. System architecture", styles["ReportH1"]))
    story.append(paragraph("The original design places caching in a Go gRPC unary interceptor. Redis provides both exact hash lookup and HNSW vector retrieval. A Python service provides the MiniLM embedding. The backend generation path is local Ollama in the benchmark setup. Kafka receives telemetry without being on the response path.", styles["ReportBody"]))
    story.append(Image(str(architecture), width=170 * mm, height=100 * mm))
    story.append(paragraph("Figure 1. Current architecture after adding Laya. Laya receives only the nearest cached candidate; it does not perform retrieval.", styles["ReportCaption"]))
    story.append(paragraph("Current decision order", styles["ReportH2"]))
    story.append(paragraph("1. Exact Redis lookup. An exact hit returns the stored response. 2. On an exact miss, the query is embedded and Redis returns the nearest cached query-response pair. 3. Shannon entropy supplies an independent threshold signal. 4. In candidate_band mode, candidates up to distance 0.35 can reach Laya even if Shannon rejects them. 5. Laya evaluates whether the cached response is safe for the new query. 6. Approval returns the cached response and promotes the phrasing to the exact cache; rejection falls back to generation.", styles["ReportBody"]))
    story.append(paragraph("A candidate means the nearest cached query-response pair before final acceptance. It is not automatically a cache hit. This distinction matters for pairs such as London/Tokyo, hello/goodbye, Python/Golang, and MySQL/PostgreSQL.", styles["ReportBody"]))

    story.append(PageBreak())
    story.append(paragraph("2. Baseline versus Laya results", styles["ReportH1"]))
    comparison_data = [
        ["Metric", "Without Laya", "With Laya", "Change"],
        ["Requests", str(base["n"]), str(laya["n"]), "same size"],
        ["Exact hits", str(base["exact"]), str(laya["exact"]), f"{laya['exact'] - base['exact']:+d}"],
        ["Semantic hits", str(base["semantic"]), str(laya["semantic"]), f"{laya['semantic'] - base['semantic']:+d}"],
        ["Fallback misses", str(base["miss"]), str(laya["miss"]), f"{laya['miss'] - base['miss']:+d}"],
        ["Combined hit rate", f"{base['hit_rate']:.1f}%", f"{laya['hit_rate']:.1f}%", f"{laya['hit_rate'] - base['hit_rate']:+.1f} pp"],
        ["Mean total latency", f"{base['all_latency']:.1f} ms", f"{laya['all_latency']:.1f} ms", f"{laya['all_latency'] - base['all_latency']:+.1f} ms"],
        ["Median total latency", f"{base['p50']:.1f} ms", f"{laya['p50']:.1f} ms", f"{laya['p50'] - base['p50']:+.1f} ms"],
    ]
    table = Table(comparison_data, colWidths=[50 * mm, 38 * mm, 38 * mm, 38 * mm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#d7e8f4")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f6f8fa")]),
    ]))
    story.append(table)
    story.append(Spacer(1, 8))
    story.append(Image(str(resolution_chart), width=165 * mm, height=93 * mm))
    story.append(paragraph("Figure 2. Resolution distribution recomputed from the two supplied CSV files.", styles["ReportCaption"]))
    story.append(paragraph("The key result is that Laya changes the composition of the semantic path, but the supplied run does not show a net cache-hit improvement. The new audit records 25 Laya-approved semantic hits; the old baseline has its own semantic-hit count, which should be compared only after confirming identical cache warm-up and request order.", styles["ReportBody"]))

    story.append(paragraph("3. Latency analysis", styles["ReportH1"]))
    story.append(Image(str(latency_chart), width=165 * mm, height=93 * mm))
    story.append(paragraph("Figure 3. Mean total latency by path. Laya's verifier cost is included in the new run.", styles["ReportCaption"]))
    latency_text = (
        f"The old baseline means are {base['exact_latency']:.1f} ms for exact hits, {base['semantic_latency']:.1f} ms for semantic hits, and {base['miss_latency']:.1f} ms for fallback misses. "
        f"The Laya run means are {laya['exact_latency']:.1f} ms, {laya['semantic_latency']:.1f} ms, and {laya['miss_latency']:.1f} ms respectively. Laya calls averaged {laya['laya_latency']:.1f} ms. Rejected Laya candidates averaged {laya['rejected_latency']:.1f} ms total, because the request waited for verification and then generated a fallback response. This is the main reason the current Laya policy can reduce throughput even when it improves safety."
    )
    story.append(paragraph(latency_text, styles["ReportBody"]))
    story.append(paragraph("Critical implementation observation", styles["ReportH2"]))
    story.append(paragraph("The current verifier call is launched in a goroutine but the interceptor waits for its result before returning or generating fallback. It is therefore asynchronous in implementation detail, but synchronous in request behavior. Also, exact requests currently wait for work that may be unnecessary after an exact hit, including embedding and the online Model AST request. A true Track A fast path should return as soon as the exact Redis response is available.", styles["ReportBody"]))

    story.append(PageBreak())
    story.append(paragraph("4. Laya decision analysis", styles["ReportH1"]))
    laya_table = [
        ["Laya measure", "Value"],
        ["Laya calls", str(laya["called"])],
        ["Approvals", f"{laya['approved']} ({laya['approval_rate']:.1f}%)"],
        ["Rejections", str(laya["rejected"])],
        ["Shannon accepted", str(laya["shannon"])],
        ["Candidate-band eligible", str(laya["candidate"])],
        ["Recovered beyond Shannon", str(laya["recovery_candidates"])],
        ["Mean verifier latency", f"{laya['laya_latency']:.1f} ms"],
    ]
    table = Table(laya_table, colWidths=[75 * mm, 55 * mm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eadcf4")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#faf7fc")]),
    ]))
    story.append(table)
    story.append(Spacer(1, 8))
    story.append(Image(str(diagnostics_chart), width=165 * mm, height=73 * mm))
    story.append(paragraph("Figure 4. Laya probability and distance diagnostics for the 141 verified candidates.", styles["ReportCaption"]))
    story.append(paragraph("Laya is conservative in this run: it rejects 116 of 141 candidates. That protects against many unsafe matches, but it also suppresses valid paraphrases whose scores fall just below 0.80. The threshold is not calibrated yet, so a raw value of 0.80 should not be read as a measured 80% correctness guarantee.", styles["ReportBody"]))
    story.append(paragraph("Observed false-positive patterns", styles["ReportH2"]))
    story.append(paragraph("The audit contains approvals that are unsafe at the answer level. A Spanish goodbye query was served a cached hello answer. A London population query was served a cached Tokyo answer. These failures show that sentence-level semantic similarity does not reliably preserve load-bearing entities or polarity. A verifier probability alone is not enough; the final policy needs deterministic consistency checks for entities, numbers, dates, programming languages, databases, negation, and time qualifiers.", styles["ReportBody"]))
    story.append(paragraph("Observed false-negative patterns", styles["ReportH2"]))
    story.append(paragraph("The audit also rejects likely valid paraphrases such as France capital, chocolate cake, Harry Potter, Mona Lisa, coffee, REST API, and C++ queue questions. This is a recall problem. Lowering the threshold without lexical safeguards would likely recover some of these but would also increase entity-substitution errors.", styles["ReportBody"]))

    story.append(paragraph("5. Relationship to the original paper", styles["ReportH1"]))
    story.append(paragraph("The paper's original contribution is a two-track exact-plus-vector cache with Shannon AST. It reports 901 total queries: 226 exact hits, 201 semantic hits, and 474 misses, for a 47.4% combined hit rate. It also reports 633.4 ms mean semantic latency, 719.1 ms exact latency, and 4,492.7 ms miss latency in the paper figure. The Laya run should be presented as a new extension experiment, not as a replacement of those original numbers.", styles["ReportBody"]))
    story.append(paragraph("The paper already identifies dense-vector false positives as a limitation, especially for language-specific tokens such as Python versus Golang. Laya addresses this limitation partially, but the new audit demonstrates that the same issue also appears in ordinary entities and opposites such as London/Tokyo and hello/goodbye. The extension therefore strengthens the research question: can a calibrated verifier plus structured token checks improve semantic-cache precision without erasing the latency benefit?", styles["ReportBody"]))

    story.append(PageBreak())
    story.append(paragraph("6. Recommended decision policy", styles["ReportH1"]))
    story.append(Preformatted("""Exact cache hit
    -> return immediately

Vector candidate retrieval
    -> reject candidates outside retrieval band

Critical-token consistency
    -> reject entity, number, date, language, polarity, or time conflicts

Laya verification
    -> calibrated probability threshold

High confidence and consistent
    -> return cached response

Uncertain or inconsistent
    -> generate a fresh response""", styles["Code"]))
    story.append(paragraph("Recommended engineering changes", styles["ReportH2"]))
    story.append(paragraph("1. Move exact-hit return ahead of unnecessary embedding and Model AST work. 2. Keep the broad candidate band for recall, but add a lexical/structured guard before Laya approval. 3. Build a labelled validation set from the benchmark pairs and calibrate Laya probabilities. 4. Sweep thresholds on held-out data and select a point using an explicit false-positive cost. 5. Remove Model AST from the online critical path because it is recorded for comparison but does not control the final decision. 6. Report P50 and P95 latency, not only averages. 7. Compare all modes with identical Redis state: baseline, Shannon-only, Shannon-gated Laya, and candidate-band Laya. 8. Separate cold-start requests from warm-cache requests.", styles["ReportBody"]))
    story.append(paragraph("Evaluation metrics to report", styles["ReportH2"]))
    story.append(paragraph("Cache metrics: exact-hit rate, semantic-hit rate, total hit rate, and fallback rate. Safety metrics: valid semantic recall, unsafe cache precision, false-positive rate on traps, false-negative rate on paraphrases, and calibration error. Performance metrics: mean, median, P95, verifier latency, Redis latency, embedding latency, generation latency, and end-to-end latency. Operational metrics: Laya calls per request, candidate-band coverage, approval rate, cache promotion rate, and warm-up effect.", styles["ReportBody"]))
    story.append(paragraph("7. Final assessment", styles["ReportH1"]))
    story.append(paragraph("Laya is valuable in this project as a semantic safety verifier and measurement instrument. The current 300-request run proves that it can rescue candidates beyond Shannon and reject many obviously incompatible candidates. It does not yet prove that the extension improves overall performance: verification costs about one second, rejected candidates become slower than ordinary misses, valid paraphrases are often rejected, and a few high-confidence approvals are unsafe. The defensible next version is not Laya alone. It is exact caching plus broad retrieval plus deterministic critical-token checks plus calibrated Laya plus fallback generation.", styles["ReportBody"]))
    story.append(paragraph("Data sources", styles["ReportH2"]))
    story.append(paragraph("Implementation paper: implementation paper.pdf. Baseline: data/qwen.csv. Laya audit: results/laya_cache_audit_v2.csv. Historical figures supplied with the repository: hit_rates.png, ast_agreement.png, trap_rejection.png. New charts in this report were generated from the two supplied CSV files.", styles["ReportSmall"]))

    def footer(canvas, document):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.grey)
        canvas.drawString(18 * mm, 10 * mm, "FSCgRPC comparative Laya report")
        canvas.drawRightString(192 * mm, 10 * mm, f"Page {document.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(str(OUTPUT_PDF), pagesize=A4, rightMargin=16 * mm, leftMargin=16 * mm, topMargin=15 * mm, bottomMargin=16 * mm, title="Laya Comparative Performance Report")
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(OUTPUT_PDF)


if __name__ == "__main__":
    build_report()
