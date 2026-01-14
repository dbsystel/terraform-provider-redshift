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
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
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
		"redshift":   func() (*schema.Provider, error) { return testAccProvider, nil },
		"testvalues": getTestValuesProvider,
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
	unsetAndSetEnvVars(t, "REDSHIFT_HOST")
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
	unsetAndSetEnvVars(t, "AWS_REGION", "AWS_DEFAULT_REGION", "REDSHIFT_HOST")
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
		ProviderFactories: testAccProviders,
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
		envKeys[envName] = os.Getenv(envName)
		_ = os.Unsetenv(envName)
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
