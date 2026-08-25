package redshift

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// The mux refuses to serve two providers whose configuration schemas differ, so the SDK
// provider and the framework provider must be kept byte-for-byte compatible while the
// migration is in progress. Asking the muxed server for its schema is the whole check.
func TestMuxedProviderSchemasMatch(t *testing.T) {
	for _, awsRegion := range []string{"", "us-west-2"} {
		name := "without AWS_REGION"
		if awsRegion != "" {
			name = "with AWS_REGION"
		}
		t.Run(name, func(t *testing.T) {
			unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION")
			if awsRegion != "" {
				t.Setenv("AWS_REGION", awsRegion)
			}

			ctx := context.Background()
			server, err := MuxedProviderServer(ctx)
			if err != nil {
				t.Fatalf("MuxedProviderServer() returned an unexpected error: %v", err)
			}

			resp, err := server().GetProviderSchema(ctx, &tfprotov6.GetProviderSchemaRequest{})
			if err != nil {
				t.Fatalf("GetProviderSchema() returned an unexpected error: %v", err)
			}
			for _, diagnostic := range resp.Diagnostics {
				if diagnostic.Severity == tfprotov6.DiagnosticSeverityError {
					t.Errorf("GetProviderSchema() reported an error: %s: %s", diagnostic.Summary, diagnostic.Detail)
				}
			}
		})
	}
}
