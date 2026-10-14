package main

import "testing"

func TestSemanticGuardRejectsLanguageMismatch(t *testing.T) {
	queryA := "Write a script to sort an array in Python"
	queryB := "Write a script to sort an array in Golang"
	mismatch, reason := semanticGuardMismatch(queryA, queryB)
	if !mismatch || reason != "PROGRAMMING_LANGUAGE_MISMATCH" {
		t.Fatal("expected language mismatch to be rejected")
	}
}

func TestSemanticGuardRejectsNegationMismatch(t *testing.T) {
	queryA := "How do I enable Docker TLS"
	queryB := "How do I disable Docker TLS"
	mismatch, reason := semanticGuardMismatch(queryA, queryB)
	if !mismatch || reason != "POLARITY_MISMATCH" {
		t.Fatal("expected polarity mismatch to be rejected")
	}
}

func TestSemanticGuardAllowsEquivalentQueries(t *testing.T) {
	queryA := "What is the capital of Japan?"
	queryB := "Name the capital city of Japan for me."
	mismatch, _ := semanticGuardMismatch(queryA, queryB)
	if mismatch {
		t.Fatal("equivalent city queries should not be rejected")
	}
}

func TestSemanticGuardRejectsNumericMismatch(t *testing.T) {
	queryA := "What is 10 + 5?"
	queryB := "What is 100 + 5?"
	mismatch, reason := semanticGuardMismatch(queryA, queryB)
	if !mismatch || reason != "NUMBER_MISMATCH" {
		t.Fatal("expected numeric mismatch to be rejected")
	}
}

func TestSemanticGuardNormalizesAliases(t *testing.T) {
	mismatch, _ := semanticGuardMismatch(
		"How do I connect to PostgreSQL?",
		"How do I connect to Postgres?",
	)
	if mismatch {
		t.Fatal("PostgreSQL and Postgres should be treated as equivalent")
	}
}

func TestSemanticGuardDoesNotTreatOpeningWordsAsEntities(t *testing.T) {
	mismatch, _ := semanticGuardMismatch(
		"Could you explain the capital of Japan?",
		"Please explain the capital of Japan.",
	)
	if mismatch {
		t.Fatal("opening words should not create an entity mismatch")
	}
}

func TestSemanticGuardRejectsTargetedContrasts(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
	}{
		{"flavor", "How do I make chocolate cake?", "How do I make vanilla cake?"},
		{"color", "What are primary colors?", "What are secondary colors?"},
		{"order", "What is the maximum value?", "What is the minimum value?"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mismatch, _ := semanticGuardMismatch(test.a, test.b)
			if !mismatch {
				t.Fatalf("expected %s contrast to be rejected", test.name)
			}
		})
	}
}
