// This file shouldn't contain actual test cases,
// but rather common utility methods for acceptance tests.
package redshift

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
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

// skipUnlessDataAPI skips the current test unless the provider is configured to
// connect via the Redshift Data API (either a serverless workgroup or a cluster
// identifier). Use this for tests that specifically exercise Data API behaviour.
func skipUnlessDataAPI(t *testing.T) {
	if os.Getenv("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME") == "" &&
		os.Getenv("REDSHIFT_DATA_API_CLUSTER_IDENTIFIER") == "" {
		t.Skip("Skipping Data API test: set REDSHIFT_DATA_API_CLUSTER_IDENTIFIER or REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME to run.")
	}
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

var (
	testAccClientOnce  sync.Once
	testAccClientValue *Client
)

// testAccClient is the client the acceptance checks query the cluster with. It is built
// from the same environment variables the provider itself reads, which is what the
// checks used to get from the SDK provider's meta value.
func testAccClient() *Client {
	testAccClientOnce.Do(func() {
		model := &frameworkProviderModel{}
		if os.Getenv("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME") != "" || os.Getenv("REDSHIFT_DATA_API_CLUSTER_IDENTIFIER") != "" {
			model.DataApi = []frameworkDataApiModel{{}}
		}

		settings, diags := model.settings(context.Background())
		if diags.HasError() {
			panic(fmt.Sprintf("could not read the provider configuration from the environment: %v", diags))
		}

		config, err := settings.newConfig(temporaryCredentials)
		if err != nil {
			panic(fmt.Sprintf("could not build the test client: %v", err))
		}
		testAccClientValue = config.NewClient()
	})
	return testAccClientValue
}
