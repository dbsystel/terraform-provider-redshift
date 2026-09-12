package redshift

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-mux/tf5to6server"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	redshiftdatasqldriver "github.com/mmichaelb/redshift-data-sql-driver"
)

var testAccProtoV6Providers map[string]func() (tfprotov6.ProviderServer, error)

func init() {
	testAccProtoV6Providers = map[string]func() (tfprotov6.ProviderServer, error){
		"redshift": providerserver.NewProtocol6WithError(New()),
		// testvalues feeds the provider block values that are only known after apply. It
		// is still written against terraform-plugin-sdk/v2, which is fine for a helper
		// that never ships: tf5to6server serves it over the protocol the tests speak.
		"testvalues": func() (tfprotov6.ProviderServer, error) {
			provider, err := getTestValuesProvider()
			if err != nil {
				return nil, err
			}
			return tf5to6server.UpgradeServer(context.Background(), provider.GRPCProvider)
		},
	}
}

func testAccPreCheck(t *testing.T) {
	workgroupName := os.Getenv("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME")
	host := os.Getenv("REDSHIFT_HOST")
	if workgroupName != "" && host != "" {
		t.Fatal("Either REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME or REDSHIFT_HOST must be set for acceptance tests")
	}
	user := os.Getenv("REDSHIFT_USER")
	if host != "" && user == "" {
		t.Fatal("REDSHIFT_USER must be set for acceptance tests")
	}
	if v := os.Getenv("REDSHIFT_TEST_ACC_DEBUG_REDSHIFT_DATA"); v != "" {
		redshiftdatasqldriver.SetDebugLogger(log.New(os.Stdout, "[redshift-data][debug]", log.Ldate|log.Ltime|log.Lshortfile))
	}
}

// temporaryCredentialsSettingsFromEnv builds the provider settings the temporary
// credentials acceptance tests connect with, skipping the test when the environment does
// not describe a cluster to get credentials for.
func temporaryCredentialsSettingsFromEnv(t *testing.T) *providerSettings {
	clusterIdentifier := getEnvOrSkip("REDSHIFT_TEMPORARY_CREDENTIALS_CLUSTER_IDENTIFIER", t)

	sdkClient, err := stsClient(t)
	if err != nil {
		t.Skipf("Unable to load STS client due to: %s", err)
	}

	response, err := sdkClient.GetCallerIdentity(context.TODO(), nil)
	if err != nil {
		t.Skipf("Unable to get current STS identity due to: %s", err)
	}
	if response == nil {
		t.Skip("Unable to get current STS identity. Empty response.")
	}

	settings, diags := (&frameworkProviderModel{}).settings(context.Background())
	if diags.HasError() {
		t.Fatalf("Failed to read the provider configuration from the environment: %v", diags)
	}
	settings.Password = ""
	settings.Username = strings.ToLower(permanentUsername(settings.Username))
	settings.TemporaryCredentials = &temporaryCredentialsSettings{ClusterIdentifier: clusterIdentifier}
	if arn, ok := os.LookupEnv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN"); ok {
		settings.TemporaryCredentials.AssumeRole = &assumeRoleSettings{Arn: arn}
	}
	return settings
}

// connectWithSettings proves the settings describe a reachable cluster.
func connectWithSettings(t *testing.T, settings *providerSettings) {
	config, err := settings.newConfig(temporaryCredentials)
	if err != nil {
		t.Fatalf("Unable to build the configuration: %s", err)
	}
	db, err := config.NewClient().Connect()
	if err != nil {
		t.Fatalf("Unable to connect to database: %s", err)
	}
	defer db.Close()
}

func stsClient(_ *testing.T) (*sts.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	return sts.NewFromConfig(cfg), nil
}

func TestAccRedshiftTemporaryCredentials(t *testing.T) {
	unsetAndSetEnvVars(t, "REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN", "REDSHIFT_PASSWORD")
	connectWithSettings(t, temporaryCredentialsSettingsFromEnv(t))
}

func TestAccRedshiftTemporaryCredentialsAssumeRole(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN", t)
	unsetAndSetEnvVars(t, "REDSHIFT_PASSWORD")
	connectWithSettings(t, temporaryCredentialsSettingsFromEnv(t))
}

func TestAccRedshiftDataApiServerlessConnect(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME", t)
	unsetAndSetEnvVars(t, "REDSHIFT_HOST")

	model := &frameworkProviderModel{DataApi: []frameworkDataApiModel{{}}}
	settings, diags := model.settings(context.Background())
	if diags.HasError() {
		t.Fatalf("Failed to read the provider configuration from the environment: %v", diags)
	}
	connectWithSettings(t, settings)
}

func Test_settingsNewConfig(t *testing.T) {
	const tempUsername, tempPassword = "temp-user", "temp-password"
	fakeTemporaryCredentialsResolver := func(username string, s *providerSettings) (string, string, error) {
		return tempUsername, tempPassword, nil
	}
	tests := []struct {
		name     string
		settings *providerSettings
		want     *Config
		wantErr  bool
	}{
		{
			"Data API config",
			&providerSettings{
				Database: "some-database",
				DataApi:  &dataApiSettings{WorkgroupName: "some-workgroup", Region: "us-west-2"},
			},
			&Config{
				DriverName: redshiftDataDriverName,
				ConnStr:    "workgroup(some-workgroup)/some-database?region=us-west-2&transactionMode=non-transactional&requestMode=blocking",
				Database:   "some-database",
				MaxConns:   1,
			},
			false,
		},
		{
			"Data API cluster config",
			&providerSettings{
				Database: "some-database",
				DataApi:  &dataApiSettings{ClusterIdentifier: "some-cluster", Username: "some-user", Region: "us-west-2"},
			},
			&Config{
				DriverName: redshiftDataDriverName,
				ConnStr:    "some-user@cluster(some-cluster)/some-database?region=us-west-2&transactionMode=non-transactional&requestMode=blocking",
				Database:   "some-database",
				MaxConns:   1,
			},
			false,
		},
		{
			"Data API cluster config - missing username",
			&providerSettings{
				Database: "some-database",
				DataApi:  &dataApiSettings{ClusterIdentifier: "some-cluster", Region: "us-west-2"},
			},
			nil,
			true,
		},
		{
			"Data API cluster config - missing region",
			&providerSettings{
				Database: "some-database",
				DataApi:  &dataApiSettings{ClusterIdentifier: "some-cluster", Username: "some-user"},
			},
			nil,
			true,
		},
		{
			"PQ config",
			&providerSettings{
				Host:           "some-host",
				Username:       "some-user",
				Password:       "some-pw",
				Port:           4122,
				SSLMode:        "require",
				Database:       "some-database",
				MaxConnections: 10,
				ConnectTimeout: defaultConnectTimeoutInSeconds,
			},
			&Config{
				DriverName: "postgresql-proxy",
				ConnStr:    "postgres://some-user:some-pw@some-host:4122/some-database?connect_timeout=180&sslmode=require",
				Database:   "some-database",
				MaxConns:   10,
			},
			false,
		},
		{
			"PQ config - fake temporary credentials",
			&providerSettings{
				Host:                 "some-host",
				Username:             "some-user",
				Port:                 4122,
				SSLMode:              "require",
				Database:             "some-database",
				MaxConnections:       defaultProviderMaxOpenConnections,
				ConnectTimeout:       defaultConnectTimeoutInSeconds,
				TemporaryCredentials: &temporaryCredentialsSettings{ClusterIdentifier: "some-cluster"},
			},
			&Config{
				DriverName: "postgresql-proxy",
				ConnStr:    fmt.Sprintf("postgres://%s:%s@some-host:4122/some-database?connect_timeout=180&sslmode=require", tempUsername, tempPassword),
				Database:   "some-database",
				MaxConns:   defaultProviderMaxOpenConnections,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.settings.newConfig(fakeTemporaryCredentialsResolver)
			if (err != nil) != tt.wantErr {
				t.Errorf("newConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.want.ConnStr != got.ConnStr {
				t.Errorf("newConfig() ConnStr = %q, want %q", got.ConnStr, tt.want.ConnStr)
			}
			if tt.want.DriverName != got.DriverName {
				t.Errorf("newConfig() DriverName = %q, want %q", got.DriverName, tt.want.DriverName)
			}
			if tt.want.MaxConns != got.MaxConns {
				t.Errorf("newConfig() MaxConns = %d, want %d", got.MaxConns, tt.want.MaxConns)
			}
			if tt.want.Database != got.Database {
				t.Errorf("newConfig() Database = %q, want %q", got.Database, tt.want.Database)
			}
		})
	}
}

func TestAccProviderCalculatedValues_HostConfig(t *testing.T) {
	testHostValue := generateRandomObjectName("tf_acc_calc_val_host")
	providerConfig := fmt.Sprintf(`
provider "redshift" {
  host     = testvalues_value.calculated_host.result
  password = "somepassword"
}

resource "testvalues_value" "calculated_host" {
  value = %[1]q
}
`, testHostValue)
	expectedError := fmt.Sprintf(`dial tcp: lookup %s: no such host`, testHostValue)
	// no such host error should occur, not a missing attribute error
	testCalculatedProviderValues(t, providerConfig, expectedError)
}

func TestAccProviderCalculatedValues_RedshiftDataConfig(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME", t)
	testWorkgroupValue := generateRandomObjectName("tf_acc_calc_val_host")
	providerConfig := fmt.Sprintf(`
provider "redshift" {
  database = "somedb"

  data_api {
    workgroup_name = testvalues_value.calculated_workgroup.result
    region         = "us-west-2"
  }
}

resource "testvalues_value" "calculated_workgroup" {
  value = %[1]q
}
`, testWorkgroupValue)
	// redshift endpoint doesn't exist in this region error should occur, not a missing attribute error
	expectedError := "ValidationException: Redshift endpoint doesn't exist in this region."
	testCalculatedProviderValues(t, providerConfig, expectedError)
}

func testCalculatedProviderValues(t *testing.T, providerConfig string, expectedError string) {
	unsetAndSetEnvVars(t, "REDSHIFT_DATABASE", "REDSHIFT_HOST", "REDSHIFT_USER", "REDSHIFT_PASSWORD", "REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME")
	testDbName := generateRandomObjectName("tf_acc_calc_val_db")
	testDbConfig := testAccDataSourceRedshiftDatabaseConfigBasic(testDbName)
	cfg := fmt.Sprintf(`
%[1]s
%[2]s
`, providerConfig, testDbConfig)
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6Providers,
		Steps: []resource.TestStep{
			{
				Config:      cfg,
				ExpectError: regexp.MustCompile(expectedError),
			},
		},
	})
}

func unsetAndSetEnvVars(t *testing.T, envNames ...string) {
	envKeys := map[string]string{}
	for _, envName := range envNames {
		previousValue := os.Getenv(envName)
		if previousValue != "" {
			envKeys[envName] = previousValue
			_ = os.Unsetenv(envName)
		}
	}
	t.Cleanup(func() {
		for key, value := range envKeys {
			_ = os.Setenv(key, value)
		}
	})
}

type testValuesProvider struct {
	testValues map[string]interface{}
}

func (p *testValuesProvider) getProvider() *schema.Provider {
	return &schema.Provider{
		ResourcesMap: map[string]*schema.Resource{
			"testvalues_value": {
				CreateContext: func(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
					value := data.Get("value").(string)
					data.Set("result", value)
					data.SetId(value)
					p.testValues[value] = &struct{}{}
					return nil
				},
				DeleteContext: func(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
					value := data.Get("value").(string)
					delete(p.testValues, value)
					data.SetId("")
					return nil
				},
				ReadContext: func(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
					value := data.Get("value").(string)

					if _, ok := p.testValues[value]; !ok {
						data.SetId("")
						return nil
					} else {
						data.SetId(value)
						data.Set("result", value)
					}
					return nil
				},
				Schema: map[string]*schema.Schema{
					"value": {
						Type:     schema.TypeString,
						Required: true,
						ForceNew: true,
					},
					"result": {
						Type:     schema.TypeString,
						Computed: true,
					},
				},
			},
		},
	}
}

func getTestValuesProvider() (*schema.Provider, error) {
	return (&testValuesProvider{testValues: make(map[string]interface{})}).getProvider(), nil
}
