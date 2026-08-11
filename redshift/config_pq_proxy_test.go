package redshift

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/lib/pq"
)

func TestBuildSessionParameters(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
		want    string
	}{
		{
			name:    "nil map produces no options",
			options: nil,
			want:    "",
		},
		{
			name:    "empty map produces no options",
			options: map[string]string{},
			want:    "",
		},
		{
			name:    "single option",
			options: map[string]string{"statement_timeout": "30000"},
			want:    "-c statement_timeout=30000",
		},
		{
			name:    "multiple options are sorted by name",
			options: map[string]string{"query_group": "superuser", "statement_timeout": "30000", "application_name": "terraform"},
			want:    "-c application_name=terraform -c query_group=superuser -c statement_timeout=30000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildSessionParameters(tt.options); got != tt.want {
				t.Errorf("buildSessionParameters() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildConnStrFromPqConfig(t *testing.T) {
	tests := []struct {
		name           string
		connectTimeout int
		options        map[string]string
		want           string
	}{
		{
			name:           "no options omits the parameter entirely",
			connectTimeout: 180,
			options:        nil,
			want:           "postgres://myuser:mypassword@redshift.example.com:5439/mydb?connect_timeout=180&sslmode=require",
		},
		{
			name:           "connect timeout is configurable",
			connectTimeout: 15,
			options:        nil,
			want:           "postgres://myuser:mypassword@redshift.example.com:5439/mydb?connect_timeout=15&sslmode=require",
		},
		{
			name:           "zero connect timeout applies no timeout",
			connectTimeout: 0,
			options:        nil,
			want:           "postgres://myuser:mypassword@redshift.example.com:5439/mydb?connect_timeout=0&sslmode=require",
		},
		{
			name:           "options are escaped into the connection string",
			connectTimeout: 180,
			options:        map[string]string{"query_group": "superuser", "statement_timeout": "30000"},
			want:           "postgres://myuser:mypassword@redshift.example.com:5439/mydb?connect_timeout=180&options=-c+query_group%3Dsuperuser+-c+statement_timeout%3D30000&sslmode=require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildConnStrFromPqConfig("redshift.example.com", "mydb", "myuser", "mypassword", 5439, "require", tt.connectTimeout, tt.options)
			if got != tt.want {
				t.Errorf("buildConnStrFromPqConfig() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The connection string is URL encoded, but lib/pq decodes it into its own configuration
// before connecting. Session parameters survive that conversion with their spaces intact.
func TestBuildConnStrFromPqConfigSurvivesPqParsing(t *testing.T) {
	unsetAndSetEnvVars(t, "PGCONNECT_TIMEOUT", "PGOPTIONS")
	connStr := buildConnStrFromPqConfig("redshift.example.com", "mydb", "myuser", "mypassword", 5439, "require", 15, map[string]string{
		"query_group":       "superuser",
		"statement_timeout": "30000",
	})

	cfg, err := pq.NewConfig(connStr)
	if err != nil {
		t.Fatalf("pq.NewConfig() returned an unexpected error: %v", err)
	}

	want := "-c query_group=superuser -c statement_timeout=30000"
	if cfg.Options != want {
		t.Errorf("cfg.Options = %q, want %q", cfg.Options, want)
	}
	if cfg.ConnectTimeout != 15*time.Second {
		t.Errorf("cfg.ConnectTimeout = %v, want %v", cfg.ConnectTimeout, 15*time.Second)
	}
}

// PGOPTIONS applies only while the provider sends no options of its own, because lib/pq
// lets the connection string take precedence over the environment.
func TestBuildConnStrFromPqConfigLeavesPGOptionsAloneWhenUnset(t *testing.T) {
	unsetAndSetEnvVars(t, "PGCONNECT_TIMEOUT")
	t.Setenv("PGOPTIONS", "-c search_path=someschema")

	connStr := buildConnStrFromPqConfig("redshift.example.com", "mydb", "myuser", "mypassword", 5439, "require", 15, nil)
	cfg, err := pq.NewConfig(connStr)
	if err != nil {
		t.Fatalf("pq.NewConfig() returned an unexpected error: %v", err)
	}
	if cfg.Options != "-c search_path=someschema" {
		t.Errorf("cfg.Options = %q, want the value from PGOPTIONS", cfg.Options)
	}

	connStr = buildConnStrFromPqConfig("redshift.example.com", "mydb", "myuser", "mypassword", 5439, "require", 15, map[string]string{"query_group": "superuser"})
	cfg, err = pq.NewConfig(connStr)
	if err != nil {
		t.Fatalf("pq.NewConfig() returned an unexpected error: %v", err)
	}
	if cfg.Options != "-c query_group=superuser" {
		t.Errorf("cfg.Options = %q, want the configured options to replace PGOPTIONS", cfg.Options)
	}
}

func TestWarnOnShadowedPGOptions(t *testing.T) {
	unsetAndSetEnvVars(t, "PGCONNECT_TIMEOUT")
	tests := []struct {
		name      string
		pgOptions string
		options   map[string]interface{}
		wantWarn  bool
	}{
		{name: "neither set"},
		{name: "only PGOPTIONS set", pgOptions: "-c search_path=someschema"},
		{name: "only options set", options: map[string]interface{}{"query_group": "superuser"}},
		{
			name:      "both set",
			pgOptions: "-c search_path=someschema",
			options:   map[string]interface{}{"query_group": "superuser"},
			wantWarn:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PGOPTIONS", tt.pgOptions)

			raw := map[string]interface{}{}
			if tt.options != nil {
				raw["session_parameters"] = tt.options
			}
			d := schema.TestResourceDataRaw(t, Provider().Schema, raw)

			diags := warnOnShadowedPGOptions(d)
			if tt.wantWarn {
				if len(diags) != 1 || diags[0].Severity != diag.Warning {
					t.Fatalf("expected exactly one warning, got %#v", diags)
				}
				return
			}
			if len(diags) != 0 {
				t.Errorf("expected no diagnostics, got %#v", diags)
			}
		})
	}
}

// connect_timeout carries a DefaultFunc, which the SDK treats as the argument always
// being set. Declaring a conflict against data_api would therefore reject every Data API
// configuration.
func TestDataApiConfigurationHasNoConflicts(t *testing.T) {
	unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST", "PGCONNECT_TIMEOUT")

	raw := map[string]interface{}{
		"data_api": []interface{}{map[string]interface{}{
			"workgroup_name": "my-workgroup",
			"region":         "us-west-2",
		}},
	}

	for _, d := range Provider().Validate(terraform.NewResourceConfigRaw(raw)) {
		t.Errorf("unexpected diagnostic: %s: %s (path %v)", d.Summary, d.Detail, d.AttributePath)
	}
}

func TestSessionParametersConflictWithDataApi(t *testing.T) {
	unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST", "PGCONNECT_TIMEOUT")

	raw := map[string]interface{}{
		"data_api": []interface{}{map[string]interface{}{
			"workgroup_name": "my-workgroup",
			"region":         "us-west-2",
		}},
		"session_parameters": map[string]interface{}{"query_group": "superuser"},
	}

	diags := Provider().Validate(terraform.NewResourceConfigRaw(raw))
	var found bool
	for _, d := range diags {
		if strings.Contains(d.Summary, "Conflicting configuration arguments") && strings.Contains(d.Detail, "session_parameters") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a conflict naming options, got %#v", diags)
	}
}

// PGCONNECT_TIMEOUT is read for every configuration, including Data API ones that never
// open a libpq connection, so a value that cannot be used must not reject the provider.
func TestUnusableConnectTimeoutEnvIsIgnored(t *testing.T) {
	for _, value := range []string{"10s", "abc", "1.5", " 30", "-5", "300000000000000000000"} {
		t.Run(value, func(t *testing.T) {
			unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST")
			t.Setenv("PGCONNECT_TIMEOUT", value)

			raw := map[string]interface{}{
				"data_api": []interface{}{map[string]interface{}{
					"workgroup_name": "my-workgroup",
					"region":         "us-west-2",
				}},
			}
			for _, d := range Provider().Validate(terraform.NewResourceConfigRaw(raw)) {
				t.Errorf("data_api configuration rejected: %s: %s", d.Summary, d.Detail)
			}

			d := schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{"host": "redshift.example.com"})
			if got := d.Get("connect_timeout").(int); got != defaultConnectTimeoutInSeconds {
				t.Errorf("connect_timeout = %d, want the default %d", got, defaultConnectTimeoutInSeconds)
			}
		})
	}
}

// A negative timeout would disable the timeout entirely in lib/pq, which is the opposite
// of what anyone setting it intends.
func TestNegativeConnectTimeoutIsRejected(t *testing.T) {
	unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST", "PGCONNECT_TIMEOUT")

	raw := map[string]interface{}{"host": "redshift.example.com", "connect_timeout": -1}
	diags := Provider().Validate(terraform.NewResourceConfigRaw(raw))
	if len(diags) == 0 {
		t.Fatal("expected connect_timeout = -1 to be rejected, got no diagnostics")
	}
}

// Invalid options must fail `terraform validate`, with the diagnostic pointing at the
// offending key rather than surfacing later as an untargeted provider error.
func TestInvalidSessionParametersFailValidation(t *testing.T) {
	unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST", "PGCONNECT_TIMEOUT")

	raw := map[string]interface{}{
		"host":               "redshift.example.com",
		"session_parameters": map[string]interface{}{"query_group": "bad value"},
	}
	diags := Provider().Validate(terraform.NewResourceConfigRaw(raw))
	if len(diags) == 0 {
		t.Fatal("expected invalid options to be rejected at validate time, got no diagnostics")
	}
	if !strings.Contains(diags[0].Detail, "query_group") {
		t.Errorf("diagnostic does not name the offending option: %#v", diags[0])
	}
	if len(diags[0].AttributePath) == 0 {
		t.Errorf("diagnostic carries no attribute path: %#v", diags[0])
	}
}

// The error from reading options must reach the caller rather than being dropped, or an
// invalid configuration would connect with the options silently discarded.
func TestInvalidSessionParametersFailProviderConfiguration(t *testing.T) {
	unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST", "PGCONNECT_TIMEOUT")

	d := schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{
		"host":               "redshift.example.com",
		"session_parameters": map[string]interface{}{"query_group": "bad value"},
	})
	if _, err := getConfigFromResourceData(d, nil); err == nil {
		t.Fatal("expected an error for an invalid session parameter, got nil")
	}
}

// The warning must be wired into provider configuration, not merely available.
func TestProviderConfigureWarnsAboutShadowedPGOptions(t *testing.T) {
	unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST", "PGCONNECT_TIMEOUT")
	t.Setenv("PGOPTIONS", "-c search_path=someschema")

	d := schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{
		"host":               "redshift.example.com",
		"session_parameters": map[string]interface{}{"query_group": "superuser"},
	})

	_, diags := providerConfigure(context.Background(), d)
	var found bool
	for _, diagnostic := range diags {
		if diagnostic.Severity == diag.Warning && strings.Contains(diagnostic.Summary, "PGOPTIONS") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a PGOPTIONS warning from providerConfigure, got %#v", diags)
	}
}

// An explicitly configured argument takes precedence over the libpq environment variable
// that supplies its default, and over the environment variable lib/pq reads for itself.
func TestExplicitConfigurationBeatsEnvironment(t *testing.T) {
	tests := []struct {
		name              string
		pgConnectTimeout  string
		pgOptions         string
		config            map[string]interface{}
		wantConnectTimout string
		wantOptions       string
	}{
		{
			name:              "neither configured nor in the environment",
			config:            map[string]interface{}{},
			wantConnectTimout: "180",
		},
		{
			name:              "environment only",
			pgConnectTimeout:  "30",
			config:            map[string]interface{}{},
			wantConnectTimout: "30",
		},
		{
			name:              "explicit connect_timeout overrides the environment",
			pgConnectTimeout:  "30",
			config:            map[string]interface{}{"connect_timeout": 7},
			wantConnectTimout: "7",
		},
		{
			name:              "explicit options override PGOPTIONS",
			pgOptions:         "-c search_path=someschema",
			config:            map[string]interface{}{"session_parameters": map[string]interface{}{"query_group": "superuser"}},
			wantConnectTimout: "180",
			wantOptions:       "-c query_group=superuser",
		},
		{
			name:              "explicit values for both override both variables",
			pgConnectTimeout:  "30",
			pgOptions:         "-c search_path=someschema",
			config:            map[string]interface{}{"connect_timeout": 7, "session_parameters": map[string]interface{}{"query_group": "superuser"}},
			wantConnectTimout: "7",
			wantOptions:       "-c query_group=superuser",
		},
		{
			name:              "PGOPTIONS still applies when options is not configured",
			pgOptions:         "-c search_path=someschema",
			config:            map[string]interface{}{},
			wantConnectTimout: "180",
			wantOptions:       "-c search_path=someschema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetAndSetEnvVars(t, "PGCONNECT_TIMEOUT", "PGOPTIONS")
			if tt.pgConnectTimeout != "" {
				t.Setenv("PGCONNECT_TIMEOUT", tt.pgConnectTimeout)
			}
			if tt.pgOptions != "" {
				t.Setenv("PGOPTIONS", tt.pgOptions)
			}

			raw := map[string]interface{}{"host": "redshift.example.com", "username": "myuser", "database": "mydb"}
			for key, value := range tt.config {
				raw[key] = value
			}
			d := schema.TestResourceDataRaw(t, Provider().Schema, raw)

			config, err := getConfigFromPqResourceData(d, "mydb", 20, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Reading the connection string back the way lib/pq does proves the value the
			// server will actually be given, including which source won.
			cfg, err := pq.NewConfig(config.ConnStr)
			if err != nil {
				t.Fatalf("pq.NewConfig() returned an unexpected error: %v", err)
			}

			wantTimeout, _ := strconv.Atoi(tt.wantConnectTimout)
			if cfg.ConnectTimeout != time.Duration(wantTimeout)*time.Second {
				t.Errorf("ConnectTimeout = %v, want %vs", cfg.ConnectTimeout, wantTimeout)
			}
			if cfg.Options != tt.wantOptions {
				t.Errorf("Options = %q, want %q", cfg.Options, tt.wantOptions)
			}
		})
	}
}

func TestSessionParametersFromResourceData(t *testing.T) {
	unsetAndSetEnvVars(t, "PGCONNECT_TIMEOUT")
	tests := []struct {
		name        string
		options     map[string]interface{}
		want        map[string]string
		wantErrPart string
	}{
		{
			name:    "unset",
			options: nil,
			want:    nil,
		},
		{
			name:    "valid options",
			options: map[string]interface{}{"statement_timeout": "30000", "query_group": "superuser"},
			want:    map[string]string{"statement_timeout": "30000", "query_group": "superuser"},
		},
		{
			name:    "empty map behaves as unset",
			options: map[string]interface{}{},
			want:    nil,
		},
		{
			name:        "uppercase name is rejected",
			options:     map[string]interface{}{"Statement_Timeout": "30000"},
			wantErrPart: "invalid session parameter name",
		},
		{
			name:        "name containing a hyphen is rejected",
			options:     map[string]interface{}{"statement-timeout": "30000"},
			wantErrPart: "invalid session parameter name",
		},
		{
			name:        "name injecting another argument is rejected",
			options:     map[string]interface{}{"query_group=x -c statement_timeout": "1"},
			wantErrPart: "invalid session parameter name",
		},
		{
			name:        "value injecting another argument is rejected",
			options:     map[string]interface{}{"query_group": "x -c statement_timeout=1"},
			wantErrPart: "invalid value",
		},
		{
			name:        "value containing a quote is rejected",
			options:     map[string]interface{}{"query_group": "x'y"},
			wantErrPart: "invalid value",
		},
		{
			name:        "value containing a backslash is rejected",
			options:     map[string]interface{}{"query_group": `x\y`},
			wantErrPart: "invalid value",
		},
		// Redshift splits the argument list with isspace(), which treats vertical tab and
		// form feed as separators just like space and tab.
		{
			name:        "value containing a vertical tab is rejected",
			options:     map[string]interface{}{"query_group": "x\v-c\vstatement_timeout=1"},
			wantErrPart: "invalid value",
		},
		{
			name:        "value containing a form feed is rejected",
			options:     map[string]interface{}{"query_group": "x\f-c\fstatement_timeout=1"},
			wantErrPart: "invalid value",
		},
		// lib/pq writes options as a NUL terminated protocol string, so an embedded NUL
		// would silently discard every option sorting after this one.
		{
			name:        "value containing a NUL is rejected",
			options:     map[string]interface{}{"query_group": "x\x00y"},
			wantErrPart: "invalid value",
		},
		{
			name:        "empty value is rejected",
			options:     map[string]interface{}{"query_group": ""},
			wantErrPart: "invalid value",
		},
		{
			name:    "values may contain punctuation Redshift settings use",
			options: map[string]interface{}{"search_path": "public,other_schema", "timezone": "America/New_York"},
			want:    map[string]string{"search_path": "public,other_schema", "timezone": "America/New_York"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := map[string]interface{}{}
			if tt.options != nil {
				raw["session_parameters"] = tt.options
			}
			d := schema.TestResourceDataRaw(t, Provider().Schema, raw)

			got, err := sessionParametersFromResourceData(d)
			if tt.wantErrPart != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q, got nil", tt.wantErrPart)
				}
				if !strings.Contains(err.Error(), tt.wantErrPart) {
					t.Errorf("error = %v, want it to contain %q", err, tt.wantErrPart)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d options, want %d (got %v)", len(got), len(tt.want), got)
			}
			for key, want := range tt.want {
				if got[key] != want {
					t.Errorf("option %q = %q, want %q", key, got[key], want)
				}
			}
		})
	}
}

// TestAccSessionParametersAreApplied verifies against a real cluster that session parameters
// reach the server. They travel in the libpq `options` startup parameter so that they
// apply to each connection as it is made: the provider keeps no idle connections, so a
// `SET` issued after connecting would not be seen by later statements.
func TestAccSessionParametersAreApplied(t *testing.T) {
	if os.Getenv("TF_ACC") == "" {
		t.Skip("TF_ACC not set. Skipping acceptance test...")
	}
	host := getEnvOrSkip("REDSHIFT_HOST", t)
	username := getEnvOrSkip("REDSHIFT_USER", t)
	password := os.Getenv("REDSHIFT_PASSWORD")

	port := 5439
	if v := os.Getenv("REDSHIFT_PORT"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad REDSHIFT_PORT: %v", err)
		}
		port = parsed
	}
	database := os.Getenv("REDSHIFT_DATABASE")
	if database == "" {
		database = "redshift"
	}
	sslMode := os.Getenv("REDSHIFT_SSLMODE")
	if sslMode == "" {
		sslMode = "require"
	}

	config := NewPqConfig(host, database, username, password, port, sslMode, 1, 30, map[string]string{
		"statement_timeout": "31000",
		"query_group":       "terraform_acc_test",
	})

	db, err := config.NewClient().Connect()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	for setting, want := range map[string]string{
		"statement_timeout": "31000",
		"query_group":       "terraform_acc_test",
	} {
		var got string
		if err := db.QueryRow(fmt.Sprintf("SHOW %s", setting)).Scan(&got); err != nil {
			t.Fatalf("failed to read %s: %v", setting, err)
		}
		if got != want {
			t.Errorf("%s = %q, want %q", setting, got, want)
		}
	}
}
