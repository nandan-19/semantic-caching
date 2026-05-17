package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"strconv"
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

type TelemetryEvent struct {
	Timestamp        string  `json:"timestamp"`
	Query            string  `json:"query"`
	ExactMatchHit    bool    `json:"exact_match_hit"`
	SemanticMatchHit bool    `json:"semantic_match_hit"`
	DistanceScore    float64 `json:"distance_score"`
	TradLatencyMs    float64 `json:"trad_latency_ms"`
	SemLatencyMs     float64 `json:"sem_latency_ms"`
}

type server struct {
	pb.UnimplementedCoreAppServiceServer
}

func (s *server) ProcessQuery(ctx context.Context, req *pb.TextRequest) (*pb.QueryResponse, error) {
	log.Println("--> Backend logic executed (Cache Miss)")
	return &pb.QueryResponse{
		Answer: "This is the expensive backend answer for: " + req.GetText(),
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
	} else {
		log.Printf("[Telemetry] 📡 Event published to message bus.")
	}
}

func semanticCacheInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

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

	if embedErr != nil {
		log.Printf("[Interceptor] Encoder pipeline failed: %v. Bypassing cache checks.", embedErr)
		return handler(ctx, req)
	}

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

					similarityThreshold := 0.20

					if scoreFloat <= similarityThreshold {
						log.Printf("[Interceptor] 🟢 SEMANTIC HIT! Distance: %f (Matched: '%s')", scoreFloat, matchedText)
						telemetry.SemanticMatchHit = true

						if !telemetry.ExactMatchHit {
							rdb.HSet(ctx, exactKey, map[string]interface{}{
								"text":     queryStr,
								"response": cachedResponse,
								"vector":   vectorBytes,
							})
							log.Printf("[Interceptor] 🔗 Created Cache Alias for variation.")
						}

						go publishTelemetry(telemetry)

						return &pb.QueryResponse{
							Answer: cachedResponse,
							Cached: true,
						}, nil
					} else {
						log.Printf("[Interceptor] 🟡 WEAK MATCH (Distance: %f, Closest: '%s').", scoreFloat, matchedText)
					}
				}
			}
		}
	}

	log.Println("[Interceptor] 🔴 CACHE MISS. Executing heavy backend logic...")
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}

	queryResp := resp.(*pb.QueryResponse)

	err = rdb.HSet(ctx, exactKey, map[string]interface{}{
		"text":     queryStr,
		"response": queryResp.GetAnswer(),
		"vector":   vectorBytes,
	}).Err()

	if err == nil {
		log.Printf("[Interceptor] ✅ Saved new query to Redis (Key: %s).", exactKey)
	}

	go publishTelemetry(telemetry)

	return resp, err
}

func main() {
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Python Encoder: %v", err)
	}
	defer conn.Close()
	encoderClient = pb.NewEncoderServiceClient(conn)
	log.Println("Connected to Python Encoder on :50052")

	rdb = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Protocol: 2,
	})
	log.Println("Connected to Redis Vector Store on :6379")

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"),
		Topic:    "cache-telemetry",
		Balancer: &kafka.LeastBytes{},
	}
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
