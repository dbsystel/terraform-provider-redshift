package redshift

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	redshiftdatasqldriver "github.com/mmichaelb/redshift-data-sql-driver"
)

var (
	testAccProviders map[string]func() (*schema.Provider, error)
	testAccProvider  *schema.Provider
)

func init() {
	testAccProvider = Provider()
	testAccProviders = map[string]func() (*schema.Provider, error){
		"redshift": func() (*schema.Provider, error) { return testAccProvider, nil },
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_impl(t *testing.T) {
	var _ = Provider()
}

func testAccPreCheck(t *testing.T) {
	var host string
	if host = os.Getenv("REDSHIFT_HOST"); host == "" {
		t.Fatal("REDSHIFT_HOST must be set for acceptance tests")
	}
	if v := os.Getenv("REDSHIFT_USER"); v == "" {
		t.Fatal("REDSHIFT_USER must be set for acceptance tests")
	}
	if v := os.Getenv("REDSHIFT_TEST_ACC_DEBUG_REDSHIFT_DATA"); v != "" {
		redshiftdatasqldriver.SetDebugLogger(log.New(os.Stdout, "[redshift-data][debug]", log.Ldate|log.Ltime|log.Lshortfile))
	}
}

func initTemporaryCredentialsProvider(t *testing.T, provider *schema.Provider) {
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

	cfg := map[string]interface{}{
		"temporary_credentials": []interface{}{
			map[string]interface{}{
				"cluster_identifier": clusterIdentifier,
			},
		},
	}
	if arn, ok := os.LookupEnv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN"); ok {
		cfg["temporary_credentials"].([]interface{})[0].(map[string]interface{})["assume_role"] = []interface{}{
			map[string]interface{}{
				"arn": arn,
			},
		}
	}
	diagnostics := provider.Configure(context.Background(), terraform.NewResourceConfigRaw(cfg))
	if diagnostics != nil {
		if diagnostics.HasError() {
			t.Fatalf("Failed to configure temporary credentials provider: %v", diagnostics)
		}
	}
}

func stsClient(_ *testing.T) (*sts.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	return sts.NewFromConfig(cfg), nil
}

func TestAccRedshiftTemporaryCredentials(t *testing.T) {
	provider := Provider()
	assumeRoleArn := os.Getenv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN")
	defer os.Setenv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN", assumeRoleArn)
	os.Unsetenv("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN")
	prepareRedshiftTemporaryCredentialsTestCases(t, provider)
	client, ok := provider.Meta().(*Client)
	if !ok {
		t.Fatal("Unable to initialize client")
	}
	db, err := client.Connect()
	if err != nil {
		t.Fatalf("Unable to connect to database: %s", err)
	}
	defer db.Close()
}

func TestAccRedshiftTemporaryCredentialsAssumeRole(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_TEMPORARY_CREDENTIALS_ASSUME_ROLE_ARN", t)
	provider := Provider()
	prepareRedshiftTemporaryCredentialsTestCases(t, provider)
	client, ok := provider.Meta().(*Client)
	if !ok {
		t.Fatal("Unable to initialize client")
	}
	db, err := client.Connect()
	if err != nil {
		t.Fatalf("Unable to connect to database: %s", err)
	}
	defer db.Close()
}

func TestAccRedshiftDataApiServerlessConnect(t *testing.T) {
	_ = getEnvOrSkip("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME", t)
	defer unsetAndSetEnvVars("REDSHIFT_HOST")()
	provider := Provider()
	provider.Configure(context.Background(), terraform.NewResourceConfigRaw(map[string]interface{}{}))
	client, ok := provider.Meta().(*Client)
	if !ok {
		t.Fatal("Unable to initialize client")
	}
	db, err := client.Connect()
	if err != nil {
		t.Fatalf("Unable to connect to database: %s", err)
	}
	defer db.Close()
}

func prepareRedshiftTemporaryCredentialsTestCases(t *testing.T, provider *schema.Provider) {
	redshiftPassword := os.Getenv("REDSHIFT_PASSWORD")
	defer os.Setenv("REDSHIFT_PASSWORD", redshiftPassword)
	os.Unsetenv("REDSHIFT_PASSWORD")
	rawUsername := os.Getenv("REDSHIFT_USER")
	defer os.Setenv("REDSHIFT_USER", rawUsername)
	username := strings.ToLower(permanentUsername(rawUsername))
	os.Setenv("REDSHIFT_USER", username)
	initTemporaryCredentialsProvider(t, provider)
}

func Test_getConfigFromResourceData(t *testing.T) {
	defer unsetAndSetEnvVars("AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST")()
	type args struct {
		d *schema.ResourceData
	}
	const tempUsername, tempPassword = "temp-user", "temp-password"
	fakeTemporaryCredentialsResolver := func(username string, d *schema.ResourceData) (string, string, error) {
		return tempUsername, tempPassword, nil
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
		{
			"Data API config",
			args{
				d: schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{
					"database": "some-database",
					"data_api": []interface{}{
						map[string]interface{}{
							"workgroup_name": "some-workgroup",
							"region":         "us-west-2",
						},
					},
				}),
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
			"Data API config missing region",
			args{
				d: schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{
					"database": "some-database",
					"data_api": []interface{}{
						map[string]interface{}{
							"workgroup_name": "some-workgroup",
						},
					},
				}),
			},
			nil,
			true,
		},
		{
			"PQ config",
			args{
				d: schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{
					"username":        "some-user",
					"password":        "some-pw",
					"host":            "some-host",
					"port":            4122,
					"database":        "some-database",
					"sslmode":         "require",
					"max_connections": 10,
				}),
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
			args{
				d: schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{
					"username": "some-user",
					"host":     "some-host",
					"port":     4122,
					"database": "some-database",
					"sslmode":  "require",
					"temporary_credentials": []interface{}{
						map[string]interface{}{
							"cluster_identifier": "some-cluster",
						},
					},
				}),
			},
			&Config{
				DriverName: "postgresql-proxy",
				ConnStr:    fmt.Sprintf("postgres://%s:%s@some-host:4122/some-database?connect_timeout=180&sslmode=require", tempUsername, tempPassword),
				Database:   "some-database",
				MaxConns:   20,
			},
			false,
		},
		{
			"PQ config - missing host",
			args{
				d: schema.TestResourceDataRaw(t, Provider().Schema, map[string]interface{}{
					"username": "some-user",
					"port":     4122,
					"database": "some-database",
					"sslmode":  "require",
				}),
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getConfigFromResourceData(tt.args.d, fakeTemporaryCredentialsResolver)
			if (err != nil) != tt.wantErr {
				t.Errorf("getConfigFromResourceData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.want.ConnStr != got.ConnStr {
				t.Errorf("getConfigFromResourceData() ConnStr = %q, want %q", got.ConnStr, tt.want.ConnStr)
			}
			if tt.want.DriverName != got.DriverName {
				t.Errorf("getConfigFromResourceData() DriverName = %q, want %q", got.DriverName, tt.want.DriverName)
			}
			if tt.want.MaxConns != got.MaxConns {
				t.Errorf("getConfigFromResourceData() MaxConns = %d, want %d", got.MaxConns, tt.want.MaxConns)
			}
			if tt.want.Database != got.Database {
				t.Errorf("getConfigFromResourceData() Database = %d, want %d", got.MaxConns, tt.want.MaxConns)
			}
		})
	}
}

func unsetAndSetEnvVars(envName ...string) func() {
	envValues := make(map[string]string)
	for _, env := range envName {
		envValue := os.Getenv(env)
		if envValue != "" {
			envValues[env] = envValue
			os.Unsetenv(env)
		}
	}
	return func() {
		for key, value := range envValues {
			if err := os.Setenv(key, value); err != nil {
				fmt.Printf("Failed to set environment variable %s: %v\n", key, err)
			}
		}
	}
}
