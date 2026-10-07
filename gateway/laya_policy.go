package main

import (
	"context"
	"math"
	"os"
	"strconv"
	"time"

	pb "github.com/THETITAN220/FSCgRPC/gateway/proto"
)

const (
	layaAcceptanceThreshold = 0.80
	layaCandidateThreshold  = 0.35
	layaTimeout             = 5 * time.Second
)

const (
	layaPolicyBehindShannon = "behind_shannon"
	layaPolicyCandidateBand = "candidate_band"
)

type cacheDecisionPolicy struct {
	Mode                string
	CandidateThreshold  float64
	AcceptanceThreshold float64
}

var defaultCacheDecisionPolicy = loadCacheDecisionPolicy()

func loadCacheDecisionPolicy() cacheDecisionPolicy {
	mode := os.Getenv("LAYA_POLICY_MODE")
	if mode != layaPolicyBehindShannon && mode != layaPolicyCandidateBand {
		mode = layaPolicyCandidateBand
	}

	return cacheDecisionPolicy{
		Mode:                mode,
		CandidateThreshold:  policyFloat("LAYA_CANDIDATE_THRESHOLD", layaCandidateThreshold),
		AcceptanceThreshold: policyFloat("LAYA_ACCEPTANCE_THRESHOLD", layaAcceptanceThreshold),
	}
}

func policyFloat(name string, fallback float64) float64 {
	value, err := strconv.ParseFloat(os.Getenv(name), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return fallback
	}
	return value
}

func (p cacheDecisionPolicy) shannonAccepted(distance, shannonThreshold float64) bool {
	return distance <= shannonThreshold
}

func (p cacheDecisionPolicy) candidateEligible(distance, shannonThreshold float64) bool {
	if p.Mode == layaPolicyBehindShannon {
		return p.shannonAccepted(distance, shannonThreshold)
	}
	return p.shannonAccepted(distance, shannonThreshold) || distance <= p.CandidateThreshold
}

func (p cacheDecisionPolicy) combine(distance, shannonThreshold, probability float64, verifyErr error) (bool, string) {
	if !p.candidateEligible(distance, shannonThreshold) {
		return false, "outside_candidate_band"
	}
	if verifyErr != nil {
		return false, "verifier_unavailable"
	}
	if probability < p.AcceptanceThreshold {
		return false, "laya_rejected"
	}
	return true, "laya_approved"
}

func layaApproved(verifyErr error, probability float64) bool {
	return verifyErr == nil && probability >= defaultCacheDecisionPolicy.AcceptanceThreshold
}

type layaVerificationResult struct {
	Response *pb.CacheVerificationResponse
	Err      error
	Latency  time.Duration
}

func verifyCachedResponseAsync(
	ctx context.Context,
	client pb.EncoderServiceClient,
	request *pb.CacheVerificationRequest,
) <-chan layaVerificationResult {
	resultCh := make(chan layaVerificationResult, 1)
	go func() {
		start := time.Now()
		verifyCtx, cancel := context.WithTimeout(ctx, layaTimeout)
		defer cancel()
		response, err := client.VerifyCachedResponse(verifyCtx, request)
		resultCh <- layaVerificationResult{
			Response: response,
			Err:      err,
			Latency:  time.Since(start),
		}
	}()
	return resultCh
}