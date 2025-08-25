// This file shouldn't contain actual test cases,
// but rather common utility methods for acceptance tests.
package redshift

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
)

// Get the value of an environment variable, or skip the
// current test if the variable is not set.
func getEnvOrSkip(key string, t *testing.T) string {
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("Environment variable %s was not set. Skipping...", key)
	}
	return v
}

// Renders a string slice as a terraform array
func tfArray(s []string) string {
	semiformat := fmt.Sprintf("%q\n", s)
	tokens := strings.Split(semiformat, " ")
	return strings.Join(tokens, ",")
}

func generateRandomObjectName(prefix string) string {
	return strings.ReplaceAll(acctest.RandomWithPrefix(prefix), "-", "_")
}
