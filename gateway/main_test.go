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
