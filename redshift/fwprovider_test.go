package redshift

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
)

// providerConfigValue builds a provider configuration from the given arguments, filling
// in a null for every argument the caller did not name.
func providerConfigValue(t *testing.T, arguments map[string]interface{}) (tfprotov6.DynamicValue, tftypes.Type) {
	t.Helper()

	server := providerserver.NewProtocol6(New())()
	schemaResponse, err := server.GetProviderSchema(context.Background(), &tfprotov6.GetProviderSchemaRequest{})
	if err != nil {
		t.Fatalf("GetProviderSchema() returned an unexpected error: %v", err)
	}

	configType := schemaResponse.Provider.ValueType()
	raw := map[string]interface{}{}
	for name := range configType.(tftypes.Object).AttributeTypes {
		raw[name] = nil
	}
	for name, value := range arguments {
		if _, ok := raw[name]; !ok {
			t.Fatalf("provider has no argument named %q", name)
		}
		raw[name] = value
	}

	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("could not encode the configuration: %v", err)
	}

	value, err := tftypes.ValueFromJSON(encoded, configType)
	if err != nil {
		t.Fatalf("could not build the configuration value: %v", err)
	}

	config, err := tfprotov6.NewDynamicValue(configType, value)
	if err != nil {
		t.Fatalf("could not encode the configuration value: %v", err)
	}
	return config, configType
}

// validateProviderConfig runs the provider's configuration validation, which is what
// `terraform validate` triggers.
func validateProviderConfig(t *testing.T, arguments map[string]interface{}) []*tfprotov6.Diagnostic {
	t.Helper()

	config, _ := providerConfigValue(t, arguments)
	server := providerserver.NewProtocol6(New())()
	resp, err := server.ValidateProviderConfig(context.Background(), &tfprotov6.ValidateProviderConfigRequest{Config: &config})
	if err != nil {
		t.Fatalf("ValidateProviderConfig() returned an unexpected error: %v", err)
	}
	return resp.Diagnostics
}

// configureProvider runs the provider's configuration, which builds the database client.
func configureProvider(t *testing.T, arguments map[string]interface{}) []*tfprotov6.Diagnostic {
	t.Helper()

	config, _ := providerConfigValue(t, arguments)
	server := providerserver.NewProtocol6(New())()
	resp, err := server.ConfigureProvider(context.Background(), &tfprotov6.ConfigureProviderRequest{Config: &config})
	if err != nil {
		t.Fatalf("ConfigureProvider() returned an unexpected error: %v", err)
	}
	return resp.Diagnostics
}

func errorDiagnostics(diagnostics []*tfprotov6.Diagnostic) []*tfprotov6.Diagnostic {
	var errors []*tfprotov6.Diagnostic
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == tfprotov6.DiagnosticSeverityError {
			errors = append(errors, diagnostic)
		}
	}
	return errors
}

// The provider schema must be internally consistent; the framework reports schema
// mistakes when it is asked for the schema.
func TestProvider(t *testing.T) {
	server := providerserver.NewProtocol6(New())()
	resp, err := server.GetProviderSchema(context.Background(), &tfprotov6.GetProviderSchemaRequest{})
	if err != nil {
		t.Fatalf("GetProviderSchema() returned an unexpected error: %v", err)
	}
	for _, diagnostic := range errorDiagnostics(resp.Diagnostics) {
		t.Errorf("GetProviderSchema() reported an error: %s: %s", diagnostic.Summary, diagnostic.Detail)
	}
}
