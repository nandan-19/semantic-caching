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

type TelemetryEvent struct {
	Timestamp        string  `json:"timestamp"`
	Query            string  `json:"query"`
	Model            string  `json:"model"`
	Response         string  `json:"response"`
	ExactMatchHit    bool    `json:"exact_match_hit"`
	SemanticMatchHit bool    `json:"semantic_match_hit"`
	DistanceScore    float64 `json:"distance_score"`
	TradLatencyMs    float64 `json:"trad_latency_ms"`
	SemLatencyMs     float64 `json:"sem_latency_ms"`
	TotalLatencyMs   float64 `json:"total_latency_ms"`
	ShannonThresh    float64 `json:"shannon_thresh"`
	ModelThresh      float64 `json:"model_thresh"`
	LayaCalled       bool    `json:"laya_called"`
	LayaProbability  float64 `json:"laya_probability"`
	LayaThreshold    float64 `json:"laya_threshold"`
	LayaDecision     string  `json:"laya_decision"`
	LayaLatencyMs    float64 `json:"laya_latency_ms"`
	ShannonAccepted  bool    `json:"shannon_accepted"`
	CandidateEligible bool   `json:"candidate_eligible"`
	DecisionPolicy   string  `json:"decision_policy"`
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
	if err != nil || resp.StatusCode != http.StatusOK {
		return 0.15
	}
	defer resp.Body.Close()

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

	exactKey := fmt.Sprintf("cache:%s", hex.EncodeToString(func(b [32]byte) []byte { return b[:] }(sha256.Sum256([]byte(queryStr)))))

	var exactRes string
	var embedRes *pb.EmbeddingResponse
	var exactLatency, semLatency float64
	var exactErr, embedErr error
	var shannonVal, modelVal float64

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		t := time.Now()
		exactRes, exactErr = rdb.HGet(ctx, exactKey, "response").Result()
		exactLatency = float64(time.Since(t).Microseconds()) / 1000.0
	}()
	go func() {
		defer wg.Done()
		t := time.Now()
		embedRes, embedErr = encoderClient.GetEmbedding(ctx, &pb.TextRequest{Text: queryStr})
		semLatency = float64(time.Since(t).Microseconds()) / 1000.0
	}()
	go func() { defer wg.Done(); shannonVal = getShannonThreshold(queryStr) }()
	go func() { defer wg.Done(); modelVal = getModelThreshold(queryStr, modelName) }()

	wg.Wait()

	telemetry := TelemetryEvent{
		Timestamp:     time.Now().Format(time.RFC3339),
		Query:         queryStr,
		Model:         modelName,
		DistanceScore: 2.0,
		TradLatencyMs: exactLatency,
		SemLatencyMs:  semLatency,
		ShannonThresh: shannonVal,
		ModelThresh:   modelVal,
	}

	var auditCachedQuery, auditCachedResponse, auditVerifierError string

	// TRACK A: EXACT HIT
	if exactErr == nil && exactRes != "" {
		telemetry.ExactMatchHit = true
		telemetry.Response = exactRes
		telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
		go publishTelemetry(telemetry)
		go publishLayaAudit(telemetry, auditCachedQuery, auditCachedResponse, auditVerifierError)
		return &pb.QueryResponse{Answer: exactRes, Cached: true}, nil
	}

	if embedErr == nil {
		vectorBytes := float32ToByte(embedRes.GetVector())
		res, err := rdb.Do(ctx, "FT.SEARCH", "idx:cache", "*=>[KNN 1 @vector $query_vec AS score]", "PARAMS", "2", "query_vec", vectorBytes, "RETURN", "3", "response", "score", "text", "DIALECT", "2").Result()

		if err == nil {
			if resSlice, ok := res.([]interface{}); ok && len(resSlice) >= 3 {
				if matchData, ok := resSlice[2].([]interface{}); ok && len(matchData) >= 6 {
					scoreFloat, _ := strconv.ParseFloat(fmt.Sprintf("%s", matchData[1]), 64)
					cachedResponse := fmt.Sprintf("%s", matchData[3])
					cachedQuery := fmt.Sprintf("%s", matchData[5])
					auditCachedQuery = cachedQuery
					auditCachedResponse = cachedResponse
					telemetry.DistanceScore = scoreFloat

					policy := defaultCacheDecisionPolicy
					telemetry.ShannonAccepted = policy.shannonAccepted(scoreFloat, shannonVal)
					telemetry.CandidateEligible = policy.candidateEligible(scoreFloat, shannonVal)
					telemetry.DecisionPolicy = policy.Mode

					if telemetry.CandidateEligible {
						telemetry.LayaCalled = true
						telemetry.LayaThreshold = policy.AcceptanceThreshold
						verificationResult := verifyCachedResponseAsync(ctx, encoderClient, &pb.CacheVerificationRequest{
							NewQuery:       queryStr,
							CachedQuery:    cachedQuery,
							CachedResponse: cachedResponse,
						})
						verification := <-verificationResult
						verifyErr := verification.Err
						telemetry.LayaLatencyMs = float64(verification.Latency.Microseconds()) / 1000.0
						probability := 0.0
						if verification.Response != nil {
							probability = float64(verification.Response.GetProbability())
						}
						telemetry.LayaProbability = probability
						if verifyErr != nil {
							auditVerifierError = verifyErr.Error()
						}
						approved, decision := policy.combine(scoreFloat, shannonVal, probability, verifyErr)
						telemetry.LayaDecision = decision
						if approved {
							telemetry.SemanticMatchHit = true
							telemetry.Response = cachedResponse // <-- Captured
							rdb.HSet(ctx, exactKey, map[string]interface{}{"text": queryStr, "response": cachedResponse, "vector": vectorBytes})

							telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
							go publishTelemetry(telemetry)
							go publishLayaAudit(telemetry, auditCachedQuery, auditCachedResponse, auditVerifierError)
							return &pb.QueryResponse{Answer: cachedResponse, Cached: true}, nil
						}
					}
				}
			}
		}

		// TRACK C: SLM FALLBACK
		reqBody, _ := json.Marshal(map[string]interface{}{
			"model":  modelName,
			"prompt": queryStr,
			"stream": false,
			"options": map[string]interface{}{
				"num_predict": 75,
			},
		})
		if httpResp, err := netClient.Post("http://127.0.0.1:11434/api/generate", "application/json", bytes.NewBuffer(reqBody)); err == nil && httpResp.StatusCode == http.StatusOK {
			defer httpResp.Body.Close()
			bodyBytes, _ := io.ReadAll(httpResp.Body)
			var oRes map[string]interface{}
			json.Unmarshal(bodyBytes, &oRes)

			if slmAnswer, ok := oRes["response"].(string); ok && slmAnswer != "" {
				finalAns := "🤖 [Qwen Edge] " + slmAnswer
				telemetry.Response = finalAns // <-- Captured
				rdb.HSet(ctx, exactKey, map[string]interface{}{"text": queryStr, "response": finalAns, "vector": vectorBytes})
				telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
				go publishTelemetry(telemetry)
				go publishLayaAudit(telemetry, auditCachedQuery, auditCachedResponse, auditVerifierError)
				return &pb.QueryResponse{Answer: finalAns, Cached: false}, nil
			}
		}
	}

	// TRACK D: CLOUD FALLBACK
	resp, _ := handler(ctx, req)
	queryResp := resp.(*pb.QueryResponse)
	telemetry.Response = queryResp.Answer // <-- Captured
	telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
	go publishTelemetry(telemetry)
	go publishLayaAudit(telemetry, auditCachedQuery, auditCachedResponse, auditVerifierError)
	return queryResp, nil
}

func main() {
	conn, _ := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	encoderClient = pb.NewEncoderServiceClient(conn)
	rdb = redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380", Protocol: 2})
	kafkaWriter = &kafka.Writer{Addr: kafka.TCP("127.0.0.1:9092"), Topic: "cache-telemetry", Balancer: &kafka.LeastBytes{}}
	defer kafkaWriter.Close()
	layaAuditWriter = &kafka.Writer{Addr: kafka.TCP("127.0.0.1:9092"), Topic: layaAuditTopic, Balancer: &kafka.LeastBytes{}}
	defer layaAuditWriter.Close()
	lis, _ := net.Listen("tcp", ":50051")
	s := grpc.NewServer(grpc.UnaryInterceptor(semanticCacheInterceptor))
	pb.RegisterCoreAppServiceServer(s, &server{})
	log.Printf("Gateway live on %v", lis.Addr())
	s.Serve(lis)
}
