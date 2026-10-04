package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

const layaAuditTopic = "laya-cache-audit"

// LayaAuditEvent is deliberately independent from the existing cache-telemetry
// topic so Laya audit storage cannot affect the legacy telemetry pipeline.
type LayaAuditEvent struct {
	Timestamp        string  `json:"timestamp"`
	Query            string  `json:"query"`
	Model            string  `json:"model"`
	FinalResponse    string  `json:"final_response"`
	ExactMatchHit    bool    `json:"exact_match_hit"`
	SemanticMatchHit bool    `json:"semantic_match_hit"`
	DistanceScore    float64 `json:"distance_score"`
	ShannonThreshold float64 `json:"shannon_threshold"`
	ModelThreshold   float64 `json:"model_threshold"`
	CachedQuery      string  `json:"cached_query"`
	CachedResponse   string  `json:"cached_response"`
	LayaCalled       bool    `json:"laya_called"`
	LayaProbability  float64 `json:"laya_probability"`
	LayaThreshold    float64 `json:"laya_threshold"`
	LayaDecision     string  `json:"laya_decision"`
	LayaLatencyMs    float64 `json:"laya_latency_ms"`
	VerifierError    string  `json:"verifier_error,omitempty"`
	TotalLatencyMs   float64 `json:"total_latency_ms"`
}

var layaAuditWriter *kafka.Writer

func publishLayaAudit(telemetry TelemetryEvent, cachedQuery, cachedResponse, verifierError string) {
	if layaAuditWriter == nil {
		return
	}

	payload, err := json.Marshal(LayaAuditEvent{
		Timestamp:        telemetry.Timestamp,
		Query:            telemetry.Query,
		Model:            telemetry.Model,
		FinalResponse:    telemetry.Response,
		ExactMatchHit:    telemetry.ExactMatchHit,
		SemanticMatchHit: telemetry.SemanticMatchHit,
		DistanceScore:    telemetry.DistanceScore,
		ShannonThreshold: telemetry.ShannonThresh,
		ModelThreshold:   telemetry.ModelThresh,
		CachedQuery:      cachedQuery,
		CachedResponse:   cachedResponse,
		LayaCalled:       telemetry.LayaCalled,
		LayaProbability:  telemetry.LayaProbability,
		LayaThreshold:    telemetry.LayaThreshold,
		LayaDecision:     telemetry.LayaDecision,
		LayaLatencyMs:    telemetry.LayaLatencyMs,
		VerifierError:    verifierError,
		TotalLatencyMs:   telemetry.TotalLatencyMs,
	})
	if err != nil {
		return
	}

	writeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = layaAuditWriter.WriteMessages(writeCtx, kafka.Message{Value: payload})
}
