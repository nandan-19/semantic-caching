package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/THETITAN220/FSCgRPC/gateway/proto"
)

var encoderClient pb.EncoderServiceClient
var rdb *redis.Client
var kafkaWriter *kafka.Writer
var netClient = &http.Client{Timeout: 60 * time.Second}
var cacheAdmission *cacheDecider

var semanticGuardAliases = map[string]map[string]string{
	"programming_language": {
		"python": "python", "golang": "go", "go": "go", "java": "java",
		"c++": "cpp", "cpp": "cpp", "javascript": "javascript", "js": "javascript",
		"typescript": "typescript", "ts": "typescript", "rust": "rust",
		"swift": "swift", "kotlin": "kotlin", "ruby": "ruby", "php": "php", "scala": "scala",
	},
	"framework": {
		"react": "react", "django": "django", "flask": "flask", "spring": "spring",
		"fastapi": "fastapi", "express": "express", "angular": "angular", "vue": "vue",
		"nodejs": "nodejs", "node.js": "nodejs", "laravel": "laravel", "rails": "rails",
	},
	"database": {
		"mysql": "mysql", "postgresql": "postgresql", "postgres": "postgresql",
		"mongodb": "mongodb", "redis": "redis", "sqlite": "sqlite", "oracle": "oracle",
		"cassandra": "cassandra", "elasticsearch": "elasticsearch", "cockroachdb": "cockroachdb",
	},
	"location": {
		"london": "london", "tokyo": "tokyo", "france": "france", "germany": "germany",
		"canada": "canada", "mexico": "mexico", "japan": "japan", "china": "china",
		"mars": "mars", "venus": "venus", "india": "india", "brazil": "brazil",
		"italy": "italy", "spain": "spain", "australia": "australia", "egypt": "egypt",
		"thailand": "thailand", "vietnam": "vietnam", "argentina": "argentina",
	},
	"unit": {
		"km": "km", "kg": "kg", "g": "g", "mg": "mg", "lb": "lb", "lbs": "lb",
		"cm": "cm", "m": "m", "mm": "mm", "ml": "ml", "l": "l", "inch": "inch",
		"inches": "inch", "mile": "mile", "miles": "mile",
	},
	"currency": {
		"usd": "usd", "eur": "eur", "gbp": "gbp", "inr": "inr", "cad": "cad",
		"aud": "aud", "jpy": "jpy", "ruble": "ruble", "rupee": "inr",
		"dollar": "usd", "dollars": "usd", "euro": "eur", "pound": "gbp",
		"pounds": "gbp", "yen": "jpy", "rupees": "inr",
	},
	"time": {
		"today": "today", "tomorrow": "tomorrow", "yesterday": "yesterday",
		"morning": "morning", "afternoon": "afternoon", "evening": "evening",
		"monday": "monday", "tuesday": "tuesday", "wednesday": "wednesday",
		"thursday": "thursday", "friday": "friday", "saturday": "saturday",
		"sunday": "sunday", "january": "january", "february": "february",
		"march": "march", "april": "april", "may": "may", "june": "june",
		"july": "july", "august": "august", "september": "september",
		"october": "october", "november": "november", "december": "december",
	},
	"negation": {
		"not": "negated", "never": "negated", "without": "negated", "cannot": "negated",
		"can't": "negated", "won't": "negated", "do not": "negated", "does not": "negated",
		"is not": "negated", "isn't": "negated", "doesn't": "negated", "don't": "negated",
		"no": "negated",
	},
	"polarity": {
		"enable": "enable", "disable": "disable", "start": "start", "stop": "stop",
		"on": "on", "off": "off", "allow": "allow", "deny": "deny", "open": "open",
		"close": "close", "turn on": "on", "turn off": "off",
	},
	"intent": {
		"hello": "greeting", "hi": "greeting", "goodbye": "farewell", "bye": "farewell",
		"enable": "enable", "disable": "disable", "start": "start", "stop": "stop",
		"shutdown": "shutdown", "restart": "restart",
	},
}

var semanticGuardPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\b\d+(?:[.,]\d+)?%?\b`),
	regexp.MustCompile(`\b(?:\d{1,2}:\d{2}\s?(?:am|pm)|\d{4}-\d{2}-\d{2}|\d{1,2}\s+(?:january|february|march|april|may|june|july|august|september|october|november|december))\b`),
	regexp.MustCompile(`(?:\$|₹|€|£)\s?\d+(?:[.,]\d+)?|\b(?:usd|eur|gbp|inr|cad|aud|jpy|dollar|dollars|rupee|rupees|pound|pounds|yen)\b`),
}

var semanticGuardContrastGroups = []struct {
	name  string
	terms map[string]string
}{
	{name: "ORDER_CONTRAST", terms: map[string]string{"first": "first", "second": "second", "maximum": "maximum", "minimum": "minimum", "max": "maximum", "min": "minimum"}},
	{name: "COLOR_CONTRAST", terms: map[string]string{"primary": "primary", "secondary": "secondary"}},
	{name: "FLAVOR_CONTRAST", terms: map[string]string{"chocolate": "chocolate", "vanilla": "vanilla"}},
}

func normalizeTextForGuard(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func extractSemanticGuardFeatures(text string) map[string]map[string]struct{} {
	features := map[string]map[string]struct{}{}
	for key := range semanticGuardAliases {
		features[key] = map[string]struct{}{}
	}
	features["number"] = map[string]struct{}{}
	features["contrast"] = map[string]struct{}{}

	normalized := normalizeTextForGuard(text)
	for category, aliases := range semanticGuardAliases {
		for alias, canonical := range aliases {
			pattern := regexp.MustCompile(`(?:^|[^a-z0-9])` + regexp.QuoteMeta(alias) + `(?:$|[^a-z0-9])`)
			if pattern.MatchString(normalized) {
				features[category][canonical] = struct{}{}
			}
		}
	}

	for _, re := range semanticGuardPatterns {
		for _, match := range re.FindAllString(normalized, -1) {
			match = strings.TrimSpace(match)
			if match != "" {
				features["number"][match] = struct{}{}
			}
		}
	}

	for _, group := range semanticGuardContrastGroups {
		for term, canonical := range group.terms {
			if regexp.MustCompile(`(?:^|[^a-z0-9])` + regexp.QuoteMeta(term) + `(?:$|[^a-z0-9])`).MatchString(normalized) {
				features["contrast"][group.name+":"+canonical] = struct{}{}
			}
		}
	}

	return features
}

func semanticGuardMismatch(queryA, queryB string) (bool, string) {
	featuresA := extractSemanticGuardFeatures(queryA)
	featuresB := extractSemanticGuardFeatures(queryB)

	for _, key := range []string{"programming_language", "framework", "database", "location", "number", "unit", "currency", "time", "contrast"} {
		if len(featuresA[key]) > 0 && len(featuresB[key]) > 0 && !sameSemanticGuardSet(featuresA[key], featuresB[key]) {
			return true, strings.ToUpper(key) + "_MISMATCH"
		}
	}

	negationA := featuresA["negation"]
	negationB := featuresB["negation"]
	if (len(negationA) > 0) != (len(negationB) > 0) {
		return true, "NEGATION_MISMATCH"
	}

	polarityA := featuresA["polarity"]
	polarityB := featuresB["polarity"]
	if (len(polarityA) > 0) != (len(polarityB) > 0) {
		return true, "POLARITY_MISMATCH"
	}
	if len(polarityA) > 0 && len(polarityB) > 0 && !sameSemanticGuardSet(polarityA, polarityB) {
		return true, "POLARITY_MISMATCH"
	}

	intentA := featuresA["intent"]
	intentB := featuresB["intent"]
	if len(intentA) > 0 && len(intentB) > 0 && !sameSemanticGuardSet(intentA, intentB) {
		return true, "INTENT_MISMATCH"
	}

	return false, ""
}

func sameSemanticGuardSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for token := range a {
		if _, ok := b[token]; !ok {
			return false
		}
	}
	return true
}

// 1. ADDED "Response" field to ship text to the dashboard
type TelemetryEvent struct {
	Timestamp        string  `json:"timestamp"`
	RequestID        string  `json:"request_id"`
	Query            string  `json:"query"`
	CandidateQuery   string  `json:"candidate_query"`
	Model            string  `json:"model"`
	Response         string  `json:"response"`
	CachedResponse   string  `json:"cached_response"`
	ResponseSource   string  `json:"response_source"`
	ExactMatchHit    bool    `json:"exact_match_hit"`
	SemanticMatchHit bool    `json:"semantic_match_hit"`
	GuardChecked     bool    `json:"guard_checked"`
	GuardBlocked     bool    `json:"guard_blocked"`
	GuardReason      string  `json:"guard_reason"`
	DistanceScore    float64 `json:"distance_score"`
	TradLatencyMs    float64 `json:"trad_latency_ms"`
	SemLatencyMs     float64 `json:"sem_latency_ms"`
	ExactCacheLatencyMs     float64 `json:"exact_cache_latency_ms"`
	EmbeddingLatencyMs      float64 `json:"embedding_latency_ms"`
	ShannonLatencyMs        float64 `json:"shannon_latency_ms"`
	ModelThresholdLatencyMs float64 `json:"model_threshold_latency_ms"`
	VectorSearchLatencyMs   float64 `json:"vector_search_latency_ms"`
	AdmissionLatencyMs      float64 `json:"admission_latency_ms"`
	CacheWriteLatencyMs     float64 `json:"cache_write_latency_ms"`
	EdgeGenerationLatencyMs float64 `json:"edge_generation_latency_ms"`
	CloudFallbackLatencyMs  float64 `json:"cloud_fallback_latency_ms"`
	TotalLatencyMs   float64 `json:"total_latency_ms"`
	ExactCacheStatus string  `json:"exact_cache_status"`
	EmbeddingStatus  string  `json:"embedding_status"`
	VectorSearchStatus string `json:"vector_search_status"`
	AdmissionStatus  string  `json:"admission_status"`
	CacheWriteStatus string   `json:"cache_write_status"`
	EdgeGenerationStatus string `json:"edge_generation_status"`
	CloudFallbackStatus string `json:"cloud_fallback_status"`
	ShannonThresh    float64 `json:"shannon_thresh"`
	ModelThresh      float64 `json:"model_thresh"`
	Decider          string  `json:"decider"`
	LayaChecked      bool    `json:"laya_checked"`
	LayaAccepted     bool    `json:"laya_accepted"`
	LayaDecision     string  `json:"laya_decision"`
	LayaConfidence   float64 `json:"laya_confidence"`
	LayaLatencyMs    float64 `json:"laya_latency_ms"`
}

func durationMs(started time.Time) float64 {
	return float64(time.Since(started).Microseconds()) / 1000.0
}

type server struct {
	pb.UnimplementedCoreAppServiceServer
}

func (s *server) ProcessQuery(ctx context.Context, req *pb.TextRequest) (*pb.QueryResponse, error) {
	time.Sleep(1500 * time.Millisecond) // Simulated Cloud Delay
	return &pb.QueryResponse{Answer: "☁️ [Cloud] " + req.GetText(), Cached: false}, nil
}

func float32ToByte(f []float32) []byte {
	bytes := make([]byte, len(f)*4)
	for i, v := range f {
		binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(v))
	}
	return bytes
}

func publishTelemetry(event TelemetryEvent) {
	payload, _ := json.Marshal(event)
	kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: payload})
}

func redisSearchFields(matchData []interface{}) map[string]string {
	fields := make(map[string]string)
	for i := 0; i+1 < len(matchData); i += 2 {
		field := fmt.Sprint(matchData[i])
		value := fmt.Sprint(matchData[i+1])
		fields[field] = value
	}
	return fields
}

func getShannonThreshold(query string) float64 {
	frequencies := make(map[rune]float64)
	for _, char := range query {
		frequencies[char]++
	}
	var entropy float64
	length := float64(len(query))
	for _, count := range frequencies {
		p := count / length
		entropy -= p * math.Log2(p)
	}

	if entropy > 4.1 {
		return 0.15
	} else if entropy > 3.6 {
		return 0.25
	}
	return 0.35
}

func getModelThreshold(query string, modelName string) float64 {
	prompt := fmt.Sprintf("Classify this prompt as 'TECHNICAL', 'MATH', or 'CONVERSATIONAL'. Answer ONLY with one word: %s", query)
	reqBody, _ := json.Marshal(map[string]interface{}{
		"model":  modelName,
		"prompt": prompt,
		"stream": false,
	})

	resp, err := netClient.Post("http://127.0.0.1:11434/api/generate", "application/json", bytes.NewBuffer(reqBody))
	if err != nil || resp == nil {
		return 0.15
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0.15
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(bodyBytes, &result)

	ans, ok := result["response"].(string)
	if ok {
		ansUpper := strings.ToUpper(ans)
		if strings.Contains(ansUpper, "TECHNICAL") || strings.Contains(ansUpper, "MATH") {
			return 0.15
		}
	}
	return 0.35
}

func semanticCacheInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	startTime := time.Now()
	textReq, ok := req.(*pb.TextRequest)
	if !ok {
		return handler(ctx, req)
	}

	queryStr := textReq.GetText()
	modelName := "qwen2.5:1.5b"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-model-name"); len(vals) > 0 {
			modelName = vals[0]
		}
	}

	telemetry := TelemetryEvent{
		Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
		RequestID:     fmt.Sprintf("req-%d", startTime.UnixNano()),
		Query:         queryStr,
		Model:         modelName,
		DistanceScore: 2.0,
		Decider:       string(cacheAdmission.mode),
		ExactCacheStatus: "miss",
		EmbeddingStatus: "skipped",
		VectorSearchStatus: "skipped",
		AdmissionStatus: "skipped",
		CacheWriteStatus: "skipped",
		EdgeGenerationStatus: "skipped",
		CloudFallbackStatus: "skipped",
	}

	// TRACK A: EXACT HIT
	exactKey := fmt.Sprintf("cache:%s", hex.EncodeToString(func(b [32]byte) []byte { return b[:] }(sha256.Sum256([]byte(queryStr)))))
	exactStart := time.Now()
	exactRes, exactErr := rdb.HGet(ctx, exactKey, "response").Result()
	telemetry.ExactCacheLatencyMs = durationMs(exactStart)
	telemetry.TradLatencyMs = telemetry.ExactCacheLatencyMs // Backward-compatible field name.
	if exactErr == nil && exactRes != "" {
		telemetry.ExactMatchHit = true
		telemetry.ExactCacheStatus = "hit"
		telemetry.Response = exactRes
		telemetry.CachedResponse = exactRes
		telemetry.ResponseSource = "exact_cache"
		telemetry.TotalLatencyMs = durationMs(startTime)
		go publishTelemetry(telemetry)
		return &pb.QueryResponse{Answer: exactRes, Cached: true}, nil
	}
	if exactErr != nil && exactErr != redis.Nil {
		telemetry.ExactCacheStatus = "error"
	}

	var embedRes *pb.EmbeddingResponse
	var embedErr error
	var semLatency float64
	var shannonVal float64
	var modelVal float64
	var shannonLatency float64
	var modelThresholdLatency float64
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		t := time.Now()
		embedRes, embedErr = encoderClient.GetEmbedding(ctx, &pb.TextRequest{Text: queryStr})
		semLatency = durationMs(t)
	}()
	go func() {
		defer wg.Done()
		t := time.Now()
		shannonVal = getShannonThreshold(queryStr)
		shannonLatency = durationMs(t)
	}()
	go func() {
		defer wg.Done()
		t := time.Now()
		modelVal = getModelThreshold(queryStr, modelName)
		modelThresholdLatency = durationMs(t)
	}()
	wg.Wait()

	telemetry.SemLatencyMs = semLatency
	telemetry.EmbeddingLatencyMs = semLatency
	telemetry.ShannonLatencyMs = shannonLatency
	telemetry.ModelThresholdLatencyMs = modelThresholdLatency
	if embedErr == nil {
		telemetry.EmbeddingStatus = "ok"
	} else {
		telemetry.EmbeddingStatus = "error"
	}
	telemetry.ShannonThresh = shannonVal
	telemetry.ModelThresh = modelVal
	appliedThreshold := shannonVal

	if embedErr == nil {
		vectorBytes := float32ToByte(embedRes.GetVector())
		vectorSearchStart := time.Now()
		res, err := rdb.Do(ctx, "FT.SEARCH", "idx:cache", "*=>[KNN 1 @vector $query_vec AS score]", "PARAMS", "2", "query_vec", vectorBytes, "RETURN", "3", "response", "score", "text", "DIALECT", "2").Result()
		telemetry.VectorSearchLatencyMs = durationMs(vectorSearchStart)

		if err == nil {
			telemetry.VectorSearchStatus = "miss"
			if resSlice, ok := res.([]interface{}); ok && len(resSlice) >= 3 {
				if matchData, ok := resSlice[2].([]interface{}); ok && len(matchData) >= 6 {
					fields := redisSearchFields(matchData)
					scoreFloat, _ := strconv.ParseFloat(fields["score"], 64)
					cachedResponse := fields["response"]
					cachedText := fields["text"]
					telemetry.VectorSearchStatus = "hit"
					telemetry.DistanceScore = scoreFloat
					telemetry.CandidateQuery = cachedText
					telemetry.CachedResponse = cachedResponse

					// TRACK B: semantic admission is configurable. Shannon can run alone,
					// as a Laya shortlist, or be replaced by Laya's decision.
					admissionStart := time.Now()
					decision := cacheAdmission.decide(ctx, queryStr, cachedText, scoreFloat, appliedThreshold)
					telemetry.AdmissionLatencyMs = durationMs(admissionStart)
					telemetry.GuardChecked = decision.checked
					telemetry.GuardBlocked = decision.blocked
					telemetry.GuardReason = decision.reason
					telemetry.LayaChecked = decision.layaChecked
					telemetry.LayaAccepted = decision.layaAccepted
					telemetry.LayaDecision = decision.layaDecision
					telemetry.LayaConfidence = decision.layaConfidence
					telemetry.LayaLatencyMs = decision.layaLatencyMs
					if decision.accepted {
						telemetry.AdmissionStatus = "accepted"
					} else if decision.reason == "LAYA_UNAVAILABLE" {
						telemetry.AdmissionStatus = "unavailable"
					} else {
						telemetry.AdmissionStatus = "rejected"
					}
					if decision.accepted {
						telemetry.SemanticMatchHit = true
						telemetry.Response = cachedResponse
						telemetry.ResponseSource = "semantic_cache"
						cacheWriteStart := time.Now()
						cacheWriteErr := rdb.HSet(ctx, exactKey, map[string]interface{}{"text": queryStr, "response": cachedResponse, "vector": vectorBytes}).Err()
						telemetry.CacheWriteLatencyMs = durationMs(cacheWriteStart)
						if cacheWriteErr == nil {
							telemetry.CacheWriteStatus = "ok"
						} else {
							telemetry.CacheWriteStatus = "error"
						}

						telemetry.TotalLatencyMs = durationMs(startTime)
						go publishTelemetry(telemetry)
						return &pb.QueryResponse{Answer: cachedResponse, Cached: true}, nil
					}
				}
			}
		} else {
			telemetry.VectorSearchStatus = "error"
		}

		// TRACK C: SLM FALLBACK
		reqBody, _ := json.Marshal(map[string]interface{}{
			"model":  modelName,
			"prompt": queryStr,
			"stream": false,
		})
		edgeGenerationStart := time.Now()
		httpResp, edgeErr := netClient.Post("http://127.0.0.1:11434/api/generate", "application/json", bytes.NewBuffer(reqBody))
		if edgeErr == nil && httpResp != nil {
			if httpResp.StatusCode == http.StatusOK {
				bodyBytes, _ := io.ReadAll(httpResp.Body)
				httpResp.Body.Close()
				telemetry.EdgeGenerationLatencyMs = durationMs(edgeGenerationStart)
				var oRes map[string]interface{}
				json.Unmarshal(bodyBytes, &oRes)

				if slmAnswer, ok := oRes["response"].(string); ok && slmAnswer != "" {
					finalAns := "🤖 [Qwen Edge] " + slmAnswer
					telemetry.Response = finalAns
					telemetry.ResponseSource = "edge_model"
					telemetry.EdgeGenerationStatus = "ok"
					cacheWriteStart := time.Now()
					cacheWriteErr := rdb.HSet(ctx, exactKey, map[string]interface{}{"text": queryStr, "response": finalAns, "vector": vectorBytes}).Err()
					telemetry.CacheWriteLatencyMs = durationMs(cacheWriteStart)
					if cacheWriteErr == nil {
						telemetry.CacheWriteStatus = "ok"
					} else {
						telemetry.CacheWriteStatus = "error"
					}
					telemetry.TotalLatencyMs = durationMs(startTime)
					go publishTelemetry(telemetry)
					return &pb.QueryResponse{Answer: finalAns, Cached: false}, nil
				}
			} else {
				httpResp.Body.Close()
			}
		}
		if telemetry.EdgeGenerationLatencyMs == 0 {
			telemetry.EdgeGenerationLatencyMs = durationMs(edgeGenerationStart)
		}
		telemetry.EdgeGenerationStatus = "error"
	}

	// TRACK D: CLOUD FALLBACK
	cloudFallbackStart := time.Now()
	resp, handlerErr := handler(ctx, req)
	telemetry.CloudFallbackLatencyMs = durationMs(cloudFallbackStart)
	if handlerErr != nil {
		telemetry.CloudFallbackStatus = "error"
		telemetry.TotalLatencyMs = durationMs(startTime)
		go publishTelemetry(telemetry)
		return resp, handlerErr
	}
	queryResp := resp.(*pb.QueryResponse)
	telemetry.Response = queryResp.Answer
	telemetry.ResponseSource = "cloud_fallback"
	telemetry.CloudFallbackStatus = "ok"
	telemetry.TotalLatencyMs = durationMs(startTime)
	go publishTelemetry(telemetry)
	return queryResp, nil
}

func main() {
	var err error
	cacheAdmission, err = newCacheDeciderFromEnv()
	if err != nil {
		log.Fatalf("invalid cache decider configuration: %v", err)
	}
	log.Printf("Semantic cache decider: %s", cacheAdmission.mode)
	if cacheAdmission.mode != cacheDeciderShannon {
		log.Printf("Laya verifier endpoint: %s", cacheAdmission.laya.url)
	}

	conn, _ := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	encoderClient = pb.NewEncoderServiceClient(conn)
	rdb = redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380", Protocol: 2})
	kafkaWriter = &kafka.Writer{Addr: kafka.TCP("127.0.0.1:9092"), Topic: "cache-telemetry", Balancer: &kafka.LeastBytes{}}
	defer kafkaWriter.Close()
	lis, _ := net.Listen("tcp", ":50051")
	s := grpc.NewServer(grpc.UnaryInterceptor(semanticCacheInterceptor))
	pb.RegisterCoreAppServiceServer(s, &server{})
	log.Printf("Gateway live on %v", lis.Addr())
	s.Serve(lis)
}
