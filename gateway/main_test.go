package main

import (
	"errors"
	"testing"
)

func TestLayaApprovalPolicy(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		probability float64
		want        bool
	}{
		{"accepts probability at threshold", nil, 0.80, true},
		{"rejects probability below threshold", nil, 0.79, false},
		{"fails closed on timeout or verifier failure", errors.New("deadline exceeded"), 1, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := layaApproved(test.err, test.probability); got != test.want {
				t.Fatalf("got %v; want %v", got, test.want)
			}
		})
	}
}

func TestCacheDecisionPolicyKeepsShannonIndependent(t *testing.T) {
	policy := cacheDecisionPolicy{
		Mode:                layaPolicyCandidateBand,
		CandidateThreshold:  0.35,
		AcceptanceThreshold: 0.80,
	}

	if policy.shannonAccepted(0.30, 0.25) {
		t.Fatal("Shannon should reject this candidate")
	}
	if !policy.candidateEligible(0.30, 0.25) {
		t.Fatal("Laya candidate band should still evaluate this candidate")
	}
	if approved, reason := policy.combine(0.30, 0.25, 0.84, nil); !approved || reason != "laya_approved" {
		t.Fatalf("got approved=%v reason=%q", approved, reason)
	}
}
