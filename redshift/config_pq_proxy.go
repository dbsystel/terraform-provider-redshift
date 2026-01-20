package redshift

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	_ "github.com/lib/pq"
)

type temporaryCredentialsResolverFunc func(username string, d *schema.ResourceData) (string, string, error)

func NewPqConfig(host, database, username, password string, port int, sslMode string, maxConns int) *Config {
	connStr := buildConnStrFromPqConfig(host, database, username, password, port, sslMode)
	return NewConfig(proxyDriverName, connStr, database, maxConns)
}

func buildConnStrFromPqConfig(host, database, username, password string, port int, sslMode string) string {
	params := map[string]string{}

	params["sslmode"] = sslMode
	params["connect_timeout"] = "180"

	var paramsArray []string
	for key, value := range params {
		paramsArray = append(paramsArray, fmt.Sprintf("%s=%s", key, url.QueryEscape(value)))
	}
	sort.Strings(paramsArray)

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		url.QueryEscape(username),
		url.QueryEscape(password),
		host,
		port,
		database,
		strings.Join(paramsArray, "&"),
	)
}

func getConfigFromPqResourceData(d *schema.ResourceData, database string, maxConnections int, temporaryCredentialsResolver temporaryCredentialsResolverFunc) (*Config, error) {
	var err error
	var password string
	host := d.Get("host").(string)
	if host == "" {
		return nil, fmt.Errorf("host must be specified and non-empty")
	}
	username := d.Get("username").(string)
	if username == "" {
		return nil, fmt.Errorf("username must be specified and non-empty")
	}
	port := d.Get("port").(int)
	sslMode := d.Get("sslmode").(string)
	log.Printf("[DEBUG] using username %q for authentication\n", username)
	_, useTemporaryCredentials := d.GetOk("temporary_credentials")
	if useTemporaryCredentials {
		log.Println("[DEBUG] using temporary credentials authentication")
		username, password, err = temporaryCredentialsResolver(username, d)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve temporary credentials: %w", err)
		}
		log.Printf("[DEBUG] got temporary credentials with username %s\n", username)
	} else {
		log.Println("[DEBUG] using password authentication")
		password = d.Get("password").(string)
		if password == "" {
			return nil, fmt.Errorf("password must be specified and non-empty when using password authentication")
		}
	}
	return NewPqConfig(host, database, username, password, port, sslMode, maxConnections), nil
}

// temporaryCredentials gets temporary credentials using GetClusterCredentials
func temporaryCredentials(username string, d *schema.ResourceData) (string, string, error) {
	sdkClient, err := redshiftSdkClient(d)
	if err != nil {
		return "", "", err
	}
	clusterIdentifier, clusterIdentifierIsSet := d.GetOk("temporary_credentials.0.cluster_identifier")
	if !clusterIdentifierIsSet {
		return "", "", fmt.Errorf("temporary_credentials not configured")
	}
	input := &redshift.GetClusterCredentialsInput{
		ClusterIdentifier: aws.String(clusterIdentifier.(string)),
		DbName:            aws.String(d.Get("database").(string)),
		DbUser:            aws.String(username),
	}
	if autoCreateUser, ok := d.GetOk("temporary_credentials.0.auto_create_user"); ok {
		input.AutoCreate = aws.Bool(autoCreateUser.(bool))
	}
	if dbGroups, ok := d.GetOk("temporary_credentials.0.db_groups"); ok {
		if dbGroups != nil {
			dbGroupsList := dbGroups.(*schema.Set).List()
			if len(dbGroupsList) > 0 {
				var groups []string
				for _, group := range dbGroupsList {
					if group.(string) != "" {
						groups = append(groups, group.(string))
					}
				}
				input.DbGroups = groups
			}
		}
	}
	if durationSeconds, ok := d.GetOk("temporary_credentials.0.duration_seconds"); ok {
		duration := durationSeconds.(int)
		if duration > 0 {
			input.DurationSeconds = aws.Int32(int32(duration))
		}
	}
	log.Println("[DEBUG] making GetClusterCredentials request")
	response, err := sdkClient.GetClusterCredentials(context.TODO(), input)
	if err != nil {
		return "", "", err
	}
	return aws.ToString(response.DbUser), aws.ToString(response.DbPassword), nil
}

func redshiftSdkClient(d *schema.ResourceData) (*redshift.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	if region := d.Get("temporary_credentials.0.region").(string); region != "" {
		cfg.Region = region
	}

	if _, ok := d.GetOk("temporary_credentials.0.assume_role"); ok {
		var parsedRoleArn string
		if roleArn, ok := d.GetOk("temporary_credentials.0.assume_role.0.arn"); ok {
			parsedRoleArn = roleArn.(string)
		}
		log.Printf("[DEBUG] Assuming role provided in configuration: [%s]", parsedRoleArn)
		opts := func(options *stscreds.AssumeRoleOptions) {
			options.Duration = time.Duration(defaultTemporaryCredentialsAssumeRoleDurationInSeconds) * time.Second
			if externalID, ok := d.GetOk("temporary_credentials.0.assume_role.0.external_id"); ok {
				options.ExternalID = aws.String(externalID.(string))
			}
			if sessionName, ok := d.GetOk("temporary_credentials.0.assume_role.0.session_name"); ok {
				options.RoleSessionName = sessionName.(string)
			}
		}
		stsClient := sts.NewFromConfig(cfg)
		cfg.Credentials = stscreds.NewAssumeRoleProvider(stsClient, parsedRoleArn, opts)
	}
	return redshift.NewFromConfig(cfg), nil
}
