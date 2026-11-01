package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// CacheDeciderMode controls how a vector-search candidate is admitted.
//
//   - shannon: Shannon's threshold plus the deterministic semantic guard.
//   - laya_verify: Shannon shortlists candidates; Laya verifies the shortlist.
//   - laya: Laya decides admission, with a broad vector-distance ceiling and
//     the deterministic semantic guard still protecting known critical tokens.
type CacheDeciderMode string

const (
	cacheDeciderShannon    CacheDeciderMode = "shannon"
	cacheDeciderLayaVerify CacheDeciderMode = "laya_verify"
	cacheDeciderLaya       CacheDeciderMode = "laya"
)

type cacheDecider struct {
	mode            CacheDeciderMode
	laya            *layaVerifier
	maxLayaDistance float64
}

type cacheDecision struct {
	accepted bool
	checked  bool
	blocked  bool
	reason   string

	layaChecked    bool
	layaAccepted   bool
	layaDecision   string
	layaConfidence float64
	layaLatencyMs  float64
}

func newCacheDeciderFromEnv() (*cacheDecider, error) {
	mode := CacheDeciderMode(strings.ToLower(strings.TrimSpace(os.Getenv("CACHE_DECIDER"))))
	if mode == "" {
		mode = cacheDeciderShannon
	}

	decider := &cacheDecider{
		mode:            mode,
		maxLayaDistance: envFloat("LAYA_MAX_DISTANCE", 0.60),
	}

	switch mode {
	case cacheDeciderShannon:
		return decider, nil
	case cacheDeciderLayaVerify, cacheDeciderLaya:
		decider.laya = newLayaVerifierFromEnv()
		return decider, nil
	default:
		return nil, fmt.Errorf("unsupported CACHE_DECIDER %q (use shannon, laya_verify, or laya)", mode)
	}
}

func envFloat(name string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func (d *cacheDecider) decide(ctx context.Context, query, candidate, cachedResponse string, distance, shannonThreshold float64) cacheDecision {
	if d.mode != cacheDeciderLaya && distance > shannonThreshold {
		return cacheDecision{}
	}
	if d.mode == cacheDeciderLaya && distance > d.maxLayaDistance {
		return cacheDecision{}
	}

	if mismatch, reason := semanticGuardMismatch(query, candidate); mismatch {
		return cacheDecision{checked: true, blocked: true, reason: reason}
	}

	if d.mode == cacheDeciderShannon {
		return cacheDecision{accepted: true, checked: true}
	}
	if strings.TrimSpace(cachedResponse) == "" {
		return cacheDecision{
			checked: true,
			blocked: true,
			reason:  "CACHED_RESPONSE_MISSING",
		}
	}
	verification := d.laya.verify(ctx, query, candidate, cachedResponse)
	decision := cacheDecision{
		checked:        true,
		layaChecked:    true,
		layaAccepted:   verification.accepted,
		layaDecision:   verification.decision,
		layaConfidence: verification.confidence,
		layaLatencyMs:  verification.latencyMs,
	}
	if verification.err != nil {
		decision.blocked = true
		decision.reason = "LAYA_UNAVAILABLE"
		return decision
	}
	if !verification.accepted {
		decision.blocked = true
		if verification.lowConfidence {
			decision.reason = "LAYA_LOW_CONFIDENCE"
		} else {
			decision.reason = "LAYA_MISMATCH"
		}
		return decision
	}

	decision.accepted = true
	return decision
}

type layaVerifier struct {
	url           string
	minConfidence float64
	client        *http.Client
}

func newLayaVerifierFromEnv() *layaVerifier {
	url := strings.TrimRight(strings.TrimSpace(os.Getenv("LAYA_URL")), "/")
	if url == "" {
		url = "http://127.0.0.1:8000/v1/systemone"
	}

	timeout := time.Duration(envFloat("LAYA_TIMEOUT_SECONDS", 5) * float64(time.Second))
	return &layaVerifier{
		url:           url,
		minConfidence: envFloat("LAYA_MIN_CONFIDENCE", 0.85),
		client:        &http.Client{Timeout: timeout},
	}
}

type layaVerification struct {
	accepted      bool
	lowConfidence bool
	decision      string
	confidence    float64
	latencyMs     float64
	err           error
}

func (v *layaVerifier) verify(ctx context.Context, incoming, cachedRequest, cachedResponse string) layaVerification {
	started := time.Now()
	requestBody := map[string]interface{}{
		"model": "typed-decisions",
		"state": map[string]string{
			"incoming_request": incoming,
			"cached_request":   cachedRequest,
		},
		"questions": map[string]interface{}{
			"cache_match": map[string]interface{}{
				"type":         "choice",
				"instructions": "Determine whether the incoming_query and cached_query would produce the same correct substantive answer when independently answered by a competent system. Compare their intended meaning, requested information, entities, constraints, scope, and expected result. Judge semantic equivalence, not textual similarity. Ignore differences in wording, grammar, and phrasing when the underlying information requested is equivalent. Pay particular attention to named entities, technical terms, quantities, dates, programming languages, versions, negation, and explicit constraints. If changing any of these changes the correct answer, classify the queries as different. Do not assume two queries are equivalent merely because they belong to the same topic or have similar wording. If equivalence cannot be established confidently, choose different.",
				"criteria": map[string]string{
					"A": "SAME ANSWER: Both queries request substantively equivalent information and would receive the same correct answer in meaning, including all material entities and constraints. Differences in wording or answer phrasing alone do not make them different.",
					"B": "DIFFERENT ANSWER: The queries differ in their requested information, intent, entities, constraints, scope, or expected outcome, such that their correct answers could differ materially. Also choose this when equivalence is ambiguous or cannot be established confidently.",
				},
			},
		},
		"min_confidence": v.minConfidence,
	}
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return layaVerification{latencyMs: elapsedMilliseconds(started), err: err}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.url, bytes.NewReader(payload))
	if err != nil {
		return layaVerification{latencyMs: elapsedMilliseconds(started), err: err}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.client.Do(req)
	if err != nil {
		return layaVerification{latencyMs: elapsedMilliseconds(started), err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return layaVerification{latencyMs: elapsedMilliseconds(started), err: fmt.Errorf("laya returned %s: %s", resp.Status, strings.TrimSpace(string(body)))}
	}

	var response struct {
		Answers map[string]struct {
			Choice           string   `json:"choice"`
			AnswerConfidence *float64 `json:"answer_confidence"`
			LowConfidence    bool     `json:"low_confidence"`
		} `json:"answers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return layaVerification{latencyMs: elapsedMilliseconds(started), err: err}
	}

	answer, ok := response.Answers["cache_match"]
	if !ok || answer.AnswerConfidence == nil {
		return layaVerification{latencyMs: elapsedMilliseconds(started), err: fmt.Errorf("laya response did not include cache_match confidence")}
	}

	accepted := answer.Choice == "A" && !answer.LowConfidence && *answer.AnswerConfidence >= v.minConfidence
	return layaVerification{
		accepted:      accepted,
		lowConfidence: answer.LowConfidence || *answer.AnswerConfidence < v.minConfidence,
		decision:      answer.Choice,
		confidence:    *answer.AnswerConfidence,
		latencyMs:     elapsedMilliseconds(started),
	}
}

func elapsedMilliseconds(started time.Time) float64 {
	return float64(time.Since(started).Microseconds()) / 1000.0
}
