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

	pb "github.com/THETITAN220/FSCgRPC/gateway/proto"
)

var encoderClient pb.EncoderServiceClient
var rdb *redis.Client
var kafkaWriter *kafka.Writer

var netClient = &http.Client{
	Timeout: 60 * time.Second,
}

type TelemetryEvent struct {
	Timestamp        string  `json:"timestamp"`
	Query            string  `json:"query"`
	ExactMatchHit    bool    `json:"exact_match_hit"`
	SemanticMatchHit bool    `json:"semantic_match_hit"`
	DistanceScore    float64 `json:"distance_score"`
	TradLatencyMs    float64 `json:"trad_latency_ms"`
	SemLatencyMs     float64 `json:"sem_latency_ms"`
	TotalLatencyMs   float64 `json:"total_latency_ms"`
}

type server struct {
	pb.UnimplementedCoreAppServiceServer
}

func (s *server) ProcessQuery(ctx context.Context, req *pb.TextRequest) (*pb.QueryResponse, error) {
	log.Println("--> [CLOUD FALLBACK] Executing heavy cloud API (Simulating 1.5s network delay)...")
	time.Sleep(1500 * time.Millisecond)
	return &pb.QueryResponse{
		Answer: "☁️ [Cloud LLM Output] " + req.GetText() + "\n",
		Cached: false,
	}, nil
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
	err := kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{Value: payload},
	)
	if err != nil {
		log.Printf("[Telemetry] Failed to write to Kafka: %v", err)
	}
}

func determineThreshold(query string) float64 {
	precisionKeywords := regexp.MustCompile(`(?i)\b(code|script|syntax|error|bug|panic|exact|algorithm|calculate|c\+\+|golang|python|typescript)\b`)
	if strings.Contains(query, "{") || strings.Contains(query, "[") {
		return 0.05
	}
	if precisionKeywords.MatchString(query) {
		return 0.08
	}
	return 0.20
}

func semanticCacheInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	startTime := time.Now()

	textReq, ok := req.(*pb.TextRequest)
	if !ok {
		return handler(ctx, req)
	}

	queryStr := textReq.GetText()
	log.Printf("[Interceptor] Received query: '%s'", queryStr)

	hash := sha256.Sum256([]byte(queryStr))
	safeHash := hex.EncodeToString(hash[:])
	exactKey := fmt.Sprintf("cache:%s", safeHash)

	var exactRes string
	var exactErr error
	var exactLatency float64

	var embedRes *pb.EmbeddingResponse
	var embedErr error
	var semLatency float64

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tChanStart := time.Now()
		exactRes, exactErr = rdb.HGet(ctx, exactKey, "response").Result()
		exactLatency = float64(time.Since(tChanStart).Microseconds()) / 1000.0
	}()

	go func() {
		defer wg.Done()
		tChanStart := time.Now()
		embedRes, embedErr = encoderClient.GetEmbedding(ctx, &pb.TextRequest{Text: queryStr})
		semLatency = float64(time.Since(tChanStart).Microseconds()) / 1000.0
	}()

	wg.Wait()

	telemetry := TelemetryEvent{
		Timestamp:        time.Now().Format(time.RFC3339),
		Query:            queryStr,
		ExactMatchHit:    false,
		SemanticMatchHit: false,
		DistanceScore:    2.0,
		TradLatencyMs:    exactLatency,
		SemLatencyMs:     semLatency,
	}

	if exactErr == nil && exactRes != "" {
		log.Printf("[Interceptor] ⚡ EXACT MATCH HIT!")
		telemetry.ExactMatchHit = true
	}

	if embedErr == nil {
		vectorBytes := float32ToByte(embedRes.GetVector())
		redisQuery := "*=>[KNN 1 @vector $query_vec AS score]"

		res, err := rdb.Do(ctx,
			"FT.SEARCH", "idx:cache", redisQuery,
			"PARAMS", "2", "query_vec", vectorBytes,
			"RETURN", "3", "response", "score", "text",
			"DIALECT", "2",
		).Result()

		if err == nil {
			resSlice, ok := res.([]interface{})
			if ok && len(resSlice) > 0 {
				var totalHits int64
				switch v := resSlice[0].(type) {
				case int64:
					totalHits = v
				case int:
					totalHits = int64(v)
				}

				if totalHits > 0 && len(resSlice) >= 3 {
					matchData, ok := resSlice[2].([]interface{})
					if ok {
						var scoreStr, cachedResponse, matchedText string
						for i := 0; i < len(matchData); i += 2 {
							key := fmt.Sprintf("%s", matchData[i])
							val := fmt.Sprintf("%s", matchData[i+1])
							if key == "score" {
								scoreStr = val
							} else if key == "response" {
								cachedResponse = val
							} else if key == "text" {
								matchedText = val
							}
						}

						scoreFloat, _ := strconv.ParseFloat(scoreStr, 64)
						telemetry.DistanceScore = scoreFloat

						dynamicThreshold := determineThreshold(queryStr)

						if scoreFloat <= dynamicThreshold {
							log.Printf("[Interceptor] 🟢 SEMANTIC HIT! Distance: %f (Matched: '%s')", scoreFloat, matchedText)
							telemetry.SemanticMatchHit = true

							if !telemetry.ExactMatchHit {
								rdb.HSet(ctx, exactKey, map[string]interface{}{
									"text":     queryStr,
									"response": cachedResponse,
									"vector":   vectorBytes,
								})
							}

							telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
							go publishTelemetry(telemetry)

							return &pb.QueryResponse{
								Answer: cachedResponse,
								Cached: true,
							}, nil
						}
					}
				}
			}
		}
	}

	log.Println("[Interceptor] 🔴 CACHE MISS. Routing to Qwen Local SLM...")

	ollamaURL := "http://127.0.0.1:11434/api/generate"
	requestBody, _ := json.Marshal(map[string]interface{}{
		"model":  "qwen2.5:1.5b",
		"prompt": queryStr,
		"stream": false,
	})

	httpResp, err := netClient.Post(ollamaURL, "application/json", bytes.NewBuffer(requestBody))

	if err != nil || httpResp.StatusCode != http.StatusOK {
		log.Printf("[SLM Edge] ⚠️ Local SLM unavailable. Falling back to heavy cloud API...")
		resp, _ := handler(ctx, req)
		queryResp := resp.(*pb.QueryResponse)

		telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
		go publishTelemetry(telemetry)
		return queryResp, nil
	}
	defer httpResp.Body.Close()

	bodyBytes, _ := io.ReadAll(httpResp.Body)
	var ollamaResult map[string]interface{}
	json.Unmarshal(bodyBytes, &ollamaResult)

	slmAnswer, ok := ollamaResult["response"].(string)

	if !ok || slmAnswer == "" {
		log.Printf("[SLM Edge] ⚠️ Local SLM returned empty format. Falling back to heavy cloud API...")
		resp, _ := handler(ctx, req)
		queryResp := resp.(*pb.QueryResponse)

		telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
		go publishTelemetry(telemetry)
		return queryResp, nil
	}

	log.Printf("[SLM Edge] 🤖 Qwen successfully answered!")

	queryResp := &pb.QueryResponse{
		Answer: "🤖 [Qwen Edge Model] " + slmAnswer,
		Cached: false,
	}

	if embedErr == nil {
		vectorBytes := float32ToByte(embedRes.GetVector())
		rdb.HSet(ctx, exactKey, map[string]interface{}{
			"text":     queryStr,
			"response": queryResp.GetAnswer(),
			"vector":   vectorBytes,
		})
	}

	telemetry.TotalLatencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0
	go publishTelemetry(telemetry)

	return queryResp, nil
}

func main() {
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Python Encoder: %v", err)
	}
	defer conn.Close()
	encoderClient = pb.NewEncoderServiceClient(conn)
	log.Println("Connected to Python Encoder on :50052")

	rdb = redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", Protocol: 2})
	log.Println("Connected to Redis Vector Store on :6379")

	kafkaWriter = &kafka.Writer{Addr: kafka.TCP("127.0.0.1:9092"), Topic: "cache-telemetry", Balancer: &kafka.LeastBytes{}}
	defer kafkaWriter.Close()
	log.Println("Connected to Redpanda Message Bus on :9092")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer(grpc.UnaryInterceptor(semanticCacheInterceptor))
	pb.RegisterCoreAppServiceServer(s, &server{})
	log.Printf("Go Gateway listening on %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
