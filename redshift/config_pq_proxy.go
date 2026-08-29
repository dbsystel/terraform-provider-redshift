package redshift

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	_ "github.com/lib/pq"
)

type temporaryCredentialsResolverFunc func(username string, s *providerSettings) (string, string, error)

func NewPqConfig(host, database, username, password string, port int, sslMode string, maxConns, connectTimeout int, sessionParameters map[string]string) *Config {
	connStr := buildConnStrFromPqConfig(host, database, username, password, port, sslMode, connectTimeout, sessionParameters)
	return NewConfig(proxyDriverName, connStr, database, maxConns)
}

// buildSessionParameters renders session configuration parameters into a value for the
// libpq `options` connection parameter, which Redshift applies to the session during
// startup. Names are sorted so that a given configuration always produces the same
// connection string. Returns an empty string when there is nothing to set, which keeps
// the parameter out of the connection string entirely so that PGOPTIONS still applies.
func buildSessionParameters(sessionParameters map[string]string) string {
	if len(sessionParameters) == 0 {
		return ""
	}

	names := make([]string, 0, len(sessionParameters))
	for name := range sessionParameters {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("-c %s=%s", name, sessionParameters[name]))
	}
	return strings.Join(parts, " ")
}

func buildConnStrFromPqConfig(host, database, username, password string, port int, sslMode string, connectTimeout int, sessionParameters map[string]string) string {
	params := map[string]string{}

	params["sslmode"] = sslMode
	params["connect_timeout"] = strconv.Itoa(connectTimeout)
	if renderedParameters := buildSessionParameters(sessionParameters); renderedParameters != "" {
		params["options"] = renderedParameters
	}

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

func (s *providerSettings) pqConfig(temporaryCredentialsResolver temporaryCredentialsResolverFunc) (*Config, error) {
	var err error
	username, password := s.Username, s.Password
	log.Printf("[DEBUG] using username %q for authentication\n", username)
	if s.TemporaryCredentials != nil {
		log.Println("[DEBUG] using temporary credentials authentication")
		username, password, err = temporaryCredentialsResolver(username, s)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve temporary credentials: %w", err)
		}
		log.Printf("[DEBUG] got temporary credentials with username %s\n", username)
	} else {
		log.Println("[DEBUG] using password authentication")
	}
	return NewPqConfig(s.Host, s.Database, username, password, s.Port, s.SSLMode, s.MaxConnections, s.ConnectTimeout, s.SessionParameters), nil
}

func validateSessionParameter(name, value string) error {
	if err := validateSessionParameterName(name); err != nil {
		return err
	}
	if !sessionParameterValueRegexp.MatchString(value) {
		return fmt.Errorf("invalid value %q for session parameter %q: only letters, digits and the characters _.,:/@+- are allowed", value, name)
	}
	return nil
}

// temporaryCredentials gets temporary credentials using GetClusterCredentials
func temporaryCredentials(username string, s *providerSettings) (string, string, error) {
	if s.TemporaryCredentials == nil || s.TemporaryCredentials.ClusterIdentifier == "" {
		return "", "", fmt.Errorf("temporary_credentials not configured")
	}
	sdkClient, err := redshiftSdkClient(s.TemporaryCredentials)
	if err != nil {
		return "", "", err
	}
	input := &redshift.GetClusterCredentialsInput{
		ClusterIdentifier: aws.String(s.TemporaryCredentials.ClusterIdentifier),
		DbName:            aws.String(s.Database),
		DbUser:            aws.String(username),
	}
	if s.TemporaryCredentials.AutoCreateUser {
		input.AutoCreate = aws.Bool(true)
	}
	for _, group := range s.TemporaryCredentials.DbGroups {
		if group != "" {
			input.DbGroups = append(input.DbGroups, group)
		}
	}
	if s.TemporaryCredentials.DurationSeconds > 0 {
		input.DurationSeconds = aws.Int32(int32(s.TemporaryCredentials.DurationSeconds))
	}
	log.Println("[DEBUG] making GetClusterCredentials request")
	response, err := sdkClient.GetClusterCredentials(context.TODO(), input)
	if err != nil {
		return "", "", err
	}
	return aws.ToString(response.DbUser), aws.ToString(response.DbPassword), nil
}

func redshiftSdkClient(s *temporaryCredentialsSettings) (*redshift.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	if s.Region != "" {
		cfg.Region = s.Region
	}

	if s.AssumeRole != nil {
		log.Printf("[DEBUG] Assuming role provided in configuration: [%s]", s.AssumeRole.Arn)
		opts := func(options *stscreds.AssumeRoleOptions) {
			options.Duration = time.Duration(defaultTemporaryCredentialsAssumeRoleDurationInSeconds) * time.Second
			if s.AssumeRole.ExternalID != "" {
				options.ExternalID = aws.String(s.AssumeRole.ExternalID)
			}
			if s.AssumeRole.SessionName != "" {
				options.RoleSessionName = s.AssumeRole.SessionName
			}
		}
		stsClient := sts.NewFromConfig(cfg)
		cfg.Credentials = stscreds.NewAssumeRoleProvider(stsClient, s.AssumeRole.Arn, opts)
	}
	return redshift.NewFromConfig(cfg), nil
}
