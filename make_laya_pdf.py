from pathlib import Path

from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.lib import colors

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "docs" / "laya_extension_summary.md"
OUTPUT = ROOT / "docs" / "laya_extension_summary.pdf"

styles = getSampleStyleSheet()
styles.add(ParagraphStyle(
    name="TitleCustom", parent=styles["Title"], alignment=TA_CENTER,
    fontSize=20, leading=25, spaceAfter=12,
))
styles.add(ParagraphStyle(
    name="HeadingCustom", parent=styles["Heading2"], fontSize=13,
    leading=16, spaceBefore=10, spaceAfter=6,
))
styles.add(ParagraphStyle(
    name="BodyCustom", parent=styles["BodyText"], fontSize=9.5,
    leading=13, spaceAfter=6,
))
styles.add(ParagraphStyle(
    name="SmallCustom", parent=styles["BodyText"], fontSize=8,
    leading=10,
))


def p(text, style="BodyCustom"):
    return Paragraph(text.replace("&", "&amp;"), styles[style])


def page_number(canvas, document):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.grey)
    canvas.drawRightString(195 * mm, 12 * mm, f"FSCgRPC Laya Extension | {document.page}")
    canvas.restoreState()


def build():
    doc = SimpleDocTemplate(
        str(OUTPUT), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=16 * mm, bottomMargin=18 * mm,
        title="Laya Extension to the Semantic Cache",
        author="FSCgRPC team",
    )
    story = []
    story.append(p("Laya Extension to the Semantic Cache", "TitleCustom"))
    story.append(p("A simple technical summary for the FSCgRPC team", "SmallCustom"))
    story.append(Spacer(1, 8))
    story.append(p("Purpose", "HeadingCustom"))
    story.append(p("The original system reduces repeated LLM inference with an exact cache and semantic vector search. Laya was added as a decision layer that evaluates whether a retrieved cached response is safe to reuse. The target trade-off is semantic recall, precision, and latency."))
    story.append(p("Original Architecture", "HeadingCustom"))
    story.append(p("1. A client sends a gRPC request to the Go gateway.<br/>2. Redis checks an exact SHA-256 cache key.<br/>3. On an exact miss, the Python encoder creates a 384-dimensional embedding.<br/>4. Redis RediSearch performs HNSW nearest-neighbor search.<br/>5. Shannon entropy selects a query-specific distance threshold.<br/>6. A valid cache result is returned; otherwise the request falls back to Ollama generation."))
    story.append(p("Original paper baseline: 901 queries, 47.4% combined cache hit rate, and an 85% latency reduction. Those numbers describe the pre-Laya experiment."))
    story.append(p("Laya Extension", "HeadingCustom"))
    story.append(p("The Python encoder exposes GetEmbedding and VerifyCachedResponse. Laya receives the new query, the nearest cached query, and the cached response. It returns a probability interpreted as the probability that the cached response is safe to return."))
    story.append(Preformatted("""Exact Redis lookup
    | hit -> return exact response
    | miss
    v
Embedding + HNSW search
    v
Nearest cached pair = candidate
    v
Shannon signal + candidate band
    v
Laya verification
    + approved -> cached response
    + rejected -> new generation""", styles["Code"]))
    story.append(p("What is a candidate?", "HeadingCustom"))
    story.append(p("A candidate is the nearest cached query-response pair returned by Redis before final acceptance. It is not automatically a cache hit. It may be a valid paraphrase or a dangerous near-match such as London versus Tokyo, hello versus goodbye, Python versus Golang, or MySQL versus PostgreSQL."))
    story.append(p("Policy Modes", "HeadingCustom"))
    story.append(p("behind_shannon: Laya is called only when Shannon accepts. candidate_band: Laya can also inspect candidates within a broader distance band. The current default settings are a candidate distance of 0.35 and a Laya approval threshold of 0.80."))
    story.append(PageBreak())
    story.append(p("Telemetry and Storage", "HeadingCustom"))
    story.append(p("Standard request telemetry is published to cache-telemetry for the dashboard. Detailed Laya audit events are published to laya-cache-audit. The audit records the distance, Shannon result, candidate eligibility, Laya probability and decision, Laya latency, total latency, and final response."))
    story.append(p("300-Request Results", "HeadingCustom"))
    data = [
        [p("Metric", "SmallCustom"), p("Result", "SmallCustom")],
        [p("Total requests", "SmallCustom"), p("300", "SmallCustom")],
        [p("Exact hits", "SmallCustom"), p("75", "SmallCustom")],
        [p("Laya-approved semantic hits", "SmallCustom"), p("25", "SmallCustom")],
        [p("Total cache hits", "SmallCustom"), p("100 (33.3%)", "SmallCustom")],
        [p("Laya calls", "SmallCustom"), p("141", "SmallCustom")],
        [p("Laya approvals / rejections", "SmallCustom"), p("25 / 116", "SmallCustom")],
        [p("Laya approval rate", "SmallCustom"), p("17.7%", "SmallCustom")],
        [p("Average Laya latency", "SmallCustom"), p("1,008 ms", "SmallCustom")],
        [p("Average total latency", "SmallCustom"), p("2,607 ms", "SmallCustom")],
    ]
    table = Table(data, colWidths=[95 * mm, 65 * mm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#d9eaf7")),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(table)
    story.append(Spacer(1, 8))
    story.append(p("The broader candidate band exposed 74 candidates to Laya that Shannon alone would have rejected."))
    story.append(p("What Worked", "HeadingCustom"))
    story.append(p("Laya approved useful paraphrases involving soccer rules, Moon distance, the first US president, freezing point, speed of light, CSV parsing, Go HTTP requests, and flu symptoms. This confirms that Laya can recover some matches beyond a strict Shannon gate."))
    story.append(p("Problems Found", "HeadingCustom"))
    story.append(p("False positives: a goodbye query received a hello response in Spanish, and a London population query received a Tokyo response. Both crossed the 0.80 threshold. This means Laya can overlook critical entity or intent tokens."))
    story.append(p("False negatives: many likely valid paraphrases were rejected just below 0.80, including France capital, chocolate cake, Harry Potter, Mona Lisa, coffee, REST API, and C++ queue queries."))
    story.append(p("Latency: Laya verification averaged about one second. Rejected candidates averaged about 3.9 seconds total because the request paid for Laya and then paid for fallback generation."))
    story.append(PageBreak())
    story.append(p("Technical Decisions", "HeadingCustom"))
    story.append(p("- Redis exact lookup and HNSW retrieval remain the cache foundation.<br/>- Shannon remains an independent deterministic signal.<br/>- Laya is a semantic safety verifier, not the retriever.<br/>- A broader candidate band gives Laya a chance to recover valid paraphrases.<br/>- Independent Shannon, candidate, Laya, and final decisions are logged.<br/>- The protobuf contract remains unchanged.<br/>- Laya audit data uses a separate Kafka topic."))
    story.append(p("Next Work", "HeadingCustom"))
    story.append(p("1. Add critical-token checks for entities, numbers, dates, languages, databases, frameworks, negation, and time words.<br/>2. Calibrate Laya on labelled cache-equivalence examples.<br/>3. Tune candidate and approval thresholds on held-out data.<br/>4. Return exact hits before waiting for embedding and model-threshold work.<br/>5. Remove the unused online Model AST request from the critical path.<br/>6. Measure warm/cold, P50, and P95 latency.<br/>7. Compare Shannon-only, Shannon-gated Laya, and candidate-band Laya on identical cache state.<br/>8. Fine-tune Laya on paraphrases and negative entity/constraint substitutions."))
    story.append(p("Bottom Line", "HeadingCustom"))
    story.append(p("The extension successfully adds a second semantic decision maker and better observability, but the current run does not prove an overall performance improvement. Laya improves some semantic recall, adds about one second of verification cost, and still permits critical entity substitutions. The next target is a layered policy: exact cache, broad retrieval, critical-token checks, calibrated Laya verification, and generation fallback."))
    doc.build(story, onFirstPage=page_number, onLaterPages=page_number)
    print(OUTPUT)


if __name__ == "__main__":
    build()
