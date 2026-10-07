# Laya Extension to the Semantic Cache

## Purpose

The original FSCgRPC system was designed to reduce repeated LLM inference. It combines an exact SHA-256 cache with semantic vector search. Laya was added as a decision layer to judge whether a retrieved cached response is safe to reuse.

The goal was not to replace the cache or the embedding search. The goal was to improve the trade-off between:

- semantic-cache recall: recovering valid paraphrases;
- precision: avoiding incorrect cached answers;
- latency: avoiding unnecessary LLM generation and verifier cost.

## Original Architecture

1. A client sends a gRPC `ProcessQuery` request to the Go gateway.
2. The gateway checks Redis for an exact SHA-256 cache entry.
3. On an exact miss, the Python encoder creates a 384-dimensional MiniLM embedding.
4. Redis RediSearch performs HNSW nearest-neighbor search.
5. Shannon character entropy selects a query-specific cosine-distance threshold.
6. If the candidate is accepted, the cached response is returned.
7. Otherwise, the request falls back to local Ollama generation, then a simulated cloud handler if needed.
8. Kafka receives telemetry for every request.

The original paper reported 901 queries, a 47.4% combined cache hit rate, and an 85% latency reduction relative to full generation. Those figures describe the pre-Laya system and are not directly the same experiment as the current run.

## Laya Extension

The Python encoder service now exposes two gRPC operations:

- `GetEmbedding`: creates the query embedding.
- `VerifyCachedResponse`: evaluates the new query, cached query, and cached response.

Laya returns a probability interpreted by this system as:

> the probability that the cached response is safe to return for the new query.

The Go gateway remains the central decision maker. Redis still finds the nearest candidate; Laya does not perform retrieval.

## Current Decision Flow

```text
Exact Redis lookup
    |
    +-- hit: return immediately as an exact hit
    |
    +-- miss
          |
          v
    Embedding and HNSW search
          |
          v
    Nearest cached query-response pair = candidate
          |
          v
    Shannon decision + candidate-band eligibility
          |
          v
    Laya verification
          |
          +-- approved: return cached response
          |
          +-- rejected/unavailable: generate a new response
```

## What Is a Candidate?

A candidate is the nearest cached query-response pair returned by Redis before final acceptance. It contains:

- the cached query;
- the cached response;
- the vector distance from the new query.

A candidate is not automatically a cache hit. It may be a valid paraphrase, a partial match, or a dangerous near-match such as:

- London versus Tokyo;
- hello versus goodbye;
- Python versus Golang;
- MySQL versus PostgreSQL;
- first versus second.

## Current Policy Modes

The gateway supports two modes through environment variables.

### `behind_shannon`

Laya is called only when the candidate passes the Shannon threshold. This reproduces the earlier Laya placement.

### `candidate_band`

Laya can inspect candidates that Shannon rejects but that remain within a broader retrieval band. This is the current default.

Current default values:

- Laya candidate distance band: `0.35`;
- Laya approval threshold: `0.80`.

These values are experimental policy settings, not proven optimal thresholds.

## Telemetry and Storage

The gateway publishes two Kafka streams:

- `cache-telemetry`: standard dashboard telemetry;
- `laya-cache-audit`: detailed Laya decisions and audit fields.

The audit records:

- exact and semantic hit status;
- vector distance;
- Shannon threshold;
- whether Shannon accepted the candidate;
- whether the candidate entered the Laya band;
- Laya probability and decision;
- Laya latency;
- total request latency;
- final response.

This makes it possible to compare Shannon, Laya, and the combined policy for the same request.

## Results from the 300-Request Run

| Metric | Result |
|---|---:|
| Total requests | 300 |
| Exact hits | 75 |
| Laya-approved semantic hits | 25 |
| Total cache hits | 100 |
| Overall hit rate | 33.3% |
| Laya calls | 141 |
| Laya approvals | 25 |
| Laya rejections | 116 |
| Laya approval rate | 17.7% |
| Average Laya latency | 1,008 ms |
| Average total latency | 2,607 ms |

The broader candidate band exposed 74 candidates to Laya that Shannon alone would have rejected.

## What Worked

Laya approved several useful paraphrases, including:

- soccer rules;
- distance between Earth and the Moon;
- first US president;
- freezing point of water;
- speed of light;
- CSV parsing in Python;
- Go HTTP GET requests;
- flu symptoms.

This shows that Laya can recover valid semantic matches that a strict Shannon gate would miss.

## Problems Found

### False positives

The most important failures were incorrect cached answers that passed the 0.80 threshold:

- A goodbye query received the cached hello answer in Spanish.
- A London population query received the cached Tokyo answer.

These show that Laya can treat a shared question pattern as equivalent while overlooking a critical entity or intent token.

### False negatives

Many likely valid paraphrases were rejected because their probabilities were just below 0.80. Examples included France capital, chocolate cake, Harry Potter, Mona Lisa, coffee, REST API, and C++ queue queries.

### Latency

Laya verification averaged about one second. Rejected Laya candidates averaged about 3.9 seconds total because the request paid for Laya and then paid for fallback generation.

The current goroutine makes the call asynchronous internally, but the request still waits for the result before deciding. It does not yet provide true latency overlap with generation.

### Exact-hit overhead

The gateway currently starts embedding and model-threshold work before it knows whether the exact Redis lookup will succeed. This limits the practical speed advantage of Track A. Exact hits should be able to return without waiting for unnecessary work.

## Engineering Decisions So Far

- Keep Redis exact lookup and HNSW retrieval as the cache foundation.
- Keep Shannon as an independent deterministic signal.
- Use Laya as a semantic safety verifier, not as the retriever.
- Use a broader candidate band to give Laya a chance to recover valid paraphrases.
- Record independent Shannon, candidate, Laya, and final decisions.
- Keep the protobuf service contract unchanged.
- Keep audit telemetry on a separate Kafka topic.

## Recommended Next Work

1. Add critical-token consistency checks for entities, numbers, dates, languages, databases, frameworks, negation, and temporal words.
2. Calibrate Laya on labelled cache-equivalence examples instead of assuming `0.80` is optimal.
3. Tune candidate and approval thresholds on held-out data.
4. Return exact hits before waiting for embedding and model-threshold work.
5. Remove the unused online Model AST request from the critical path.
6. Measure warm and cold latency separately, including P50 and P95.
7. Compare Shannon-only, Shannon-gated Laya, and candidate-band Laya on the same cache state.
8. Fine-tune Laya using positive paraphrase pairs and negative entity/constraint substitutions.

## Bottom Line

The Laya extension successfully adds a second semantic decision maker and improves observability. The current results do not yet prove an overall performance improvement. Laya improves some semantic recalls, but it is conservative, adds about one second of verification cost, and still permits critical entity substitutions. The next safe architecture is a layered policy: exact cache first, broad vector retrieval, critical-token checks, calibrated Laya verification, and generation fallback.
