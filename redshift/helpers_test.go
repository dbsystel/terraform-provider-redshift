package redshift

import (
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

func TestRetryOnPQErrors(t *testing.T) {
	retryableError := &pq.Error{Code: pq.ErrorCode(pqErrorCodeConcurrent)}
	persistentErrors := retryablePQErrors(10)
	nonRetryableError := errors.New("permanent failure")

	tests := []struct {
		name       string
		results    []error
		wantErr    error
		wantCalls  int
		wantSleeps []time.Duration
	}{
		{
			name:      "immediate success",
			results:   []error{nil},
			wantCalls: 1,
		},
		{
			name:       "retryable error then success",
			results:    []error{retryableError, nil},
			wantCalls:  2,
			wantSleeps: []time.Duration{time.Second},
		},
		{
			name:       "persistent retryable error returns final error",
			results:    persistentErrors,
			wantErr:    persistentErrors[9],
			wantCalls:  10,
			wantSleeps: retrySleepDurations(10),
		},
		{
			name:      "non-retryable error returns immediately",
			results:   []error{nonRetryableError, nil},
			wantErr:   nonRetryableError,
			wantCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			var sleeps []time.Duration
			err := retryOnPQErrors(func(*DBConnection, *schema.ResourceData) error {
				result := test.results[min(calls, len(test.results)-1)]
				calls++
				return result
			}, func(duration time.Duration) {
				sleeps = append(sleeps, duration)
			})

			if err := err(nil, nil); !errors.Is(err, test.wantErr) {
				t.Fatalf("retryOnPQErrors() error = %v, want %v", err, test.wantErr)
			}
			if calls != test.wantCalls {
				t.Errorf("retryOnPQErrors() called function %d times, want %d", calls, test.wantCalls)
			}
			if len(sleeps) != len(test.wantSleeps) {
				t.Fatalf("retryOnPQErrors() slept %d times, want %d", len(sleeps), len(test.wantSleeps))
			}
			for i := range sleeps {
				if sleeps[i] != test.wantSleeps[i] {
					t.Errorf("retryOnPQErrors() sleep %d = %s, want %s", i+1, sleeps[i], test.wantSleeps[i])
				}
			}
		})
	}
}

func retryablePQErrors(count int) []error {
	errors := make([]error, count)
	for i := range errors {
		errors[i] = &pq.Error{
			Code:    pq.ErrorCode(pqErrorCodeConcurrent),
			Detail:  "retry attempt",
			Message: string(rune('0' + i)),
		}
	}
	return errors
}

func retrySleepDurations(count int) []time.Duration {
	durations := make([]time.Duration, count)
	for i := range durations {
		durations[i] = time.Duration(i+1) * time.Second
	}
	return durations
}

func TestValidatePrivileges(t *testing.T) {
	tests := map[string]struct {
		privileges []string
		objectType string
		expected   bool
	}{
		"valid list for database": {
			privileges: []string{"create", "usage", "temporary", "temp", "alter"},
			objectType: "database",
			expected:   true,
		},
		"invalid list for database": {
			privileges: []string{"create", "execute"},
			objectType: "database",
			expected:   false,
		},
		"valid list for schema": {
			privileges: []string{"create", "usage", "alter", "drop"},
			objectType: "schema",
			expected:   true,
		},
		"invalid list for schema": {
			privileges: []string{"foo"},
			objectType: "schema",
			expected:   false,
		},
		"extended invalid list for schema": {
			privileges: []string{"create", "usage", "insert"},
			objectType: "schema",
			expected:   false,
		},
		"empty list for schema": {
			privileges: []string{},
			objectType: "schema",
			expected:   true,
		},
		"valid list for table": {
			privileges: []string{"select", "insert", "update", "delete", "drop", "references", "alter", "truncate"},
			objectType: "table",
			expected:   true,
		},
		"invalid list for table": {
			privileges: []string{"foobar"},
			objectType: "table",
			expected:   false,
		},
		"unsupported list for table (rule)": {
			privileges: []string{"rule"},
			objectType: "table",
			expected:   false,
		},
		"unsupported list for table (trigger)": {
			privileges: []string{"trigger"},
			objectType: "table",
			expected:   false,
		},
		"extended invalid list for table": {
			privileges: []string{"create", "usage", "insert"},
			objectType: "table",
			expected:   false,
		},
		"empty list for table": {
			privileges: []string{},
			objectType: "table",
			expected:   true,
		},
		"valid list for function": {
			privileges: []string{"execute"},
			objectType: "function",
			expected:   true,
		},
		"invalid list for function": {
			privileges: []string{"foo"},
			objectType: "function",
			expected:   false,
		},
		"extended invalid list for function": {
			privileges: []string{"execute", "foo"},
			objectType: "function",
			expected:   false,
		},
		"valid list for procedure": {
			privileges: []string{"execute"},
			objectType: "procedure",
			expected:   true,
		},
		"invalid list for procedure": {
			privileges: []string{"foo"},
			objectType: "procedure",
			expected:   false,
		},
		"extended invalid list for procedure": {
			privileges: []string{"execute", "foo"},
			objectType: "procedure",
			expected:   false,
		},
		"valid list for language": {
			privileges: []string{"usage"},
			objectType: "language",
			expected:   true,
		},
		"invalid list for language": {
			privileges: []string{"foo"},
			objectType: "language",
			expected:   false,
		},
		"extended invalid list for language": {
			privileges: []string{"usage", "foo"},
			objectType: "language",
			expected:   false,
		},
		"empty list for language": {
			privileges: []string{},
			objectType: "language",
			expected:   false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result := validatePrivileges(tt.privileges, tt.objectType)

			if result != tt.expected {
				t.Errorf("Expected result to be `%t` but got `%t`", tt.expected, result)
			}
		})
	}
}
