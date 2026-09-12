package redshift

import (
	"fmt"

	_ "github.com/mmichaelb/redshift-data-sql-driver"
)

const redshiftDataDriverName = "redshift-data"

func NewDataApiConfig(workgroupName, database, awsRegion string, maxConns int) *Config {
	connStr := buildConnStrFromDataApiConfig(workgroupName, database, awsRegion)
	return NewConfig(redshiftDataDriverName, connStr, database, maxConns)
}

func buildConnStrFromDataApiConfig(workgroupName, database, awsRegion string) string {
	return fmt.Sprintf(
		"workgroup(%s)/%s?region=%s&transactionMode=non-transactional&requestMode=blocking",
		workgroupName, database, awsRegion,
	)
}

func NewDataApiClusterConfig(clusterIdentifier, username, database, awsRegion string, maxConns int) (*Config, error) {
	if username == "" {
		return nil, fmt.Errorf("data_api configuration with cluster_identifier requires username to be set")
	}
	connStr := buildConnStrFromDataApiClusterConfig(clusterIdentifier, username, database, awsRegion)
	return NewConfig(redshiftDataDriverName, connStr, database, maxConns), nil
}

func buildConnStrFromDataApiClusterConfig(clusterIdentifier, username, database, awsRegion string) string {
	return fmt.Sprintf(
		"%s@cluster(%s)/%s?region=%s&transactionMode=non-transactional&requestMode=blocking",
		username, clusterIdentifier, database, awsRegion,
	)
}

func (s *providerSettings) dataApiConfig() (*Config, error) {
	if s.DataApi.Region == "" {
		return nil, fmt.Errorf("data_api configuration requires region to be set")
	}

	// Data API connections are non-pooled; one connection is sufficient.
	if s.DataApi.ClusterIdentifier != "" {
		return NewDataApiClusterConfig(s.DataApi.ClusterIdentifier, s.DataApi.Username, s.Database, s.DataApi.Region, 1)
	}

	if s.DataApi.WorkgroupName != "" {
		return NewDataApiConfig(s.DataApi.WorkgroupName, s.Database, s.DataApi.Region, 1), nil
	}

	return nil, fmt.Errorf("data_api configuration requires either workgroup_name or cluster_identifier to be set")
}
