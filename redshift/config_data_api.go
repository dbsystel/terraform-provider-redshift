package redshift

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
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

func getConfigFromDataApiResourceData(d *schema.ResourceData, database string) (*Config, error) {
	workgroupName := d.Get("data_api.0.workgroup_name").(string)
	if workgroupName == "" {
		return nil, fmt.Errorf(`attribute "workgroup_name" is required in data_api configuration`)
	}
	region := d.Get("data_api.0.region").(string)
	if region == "" {
		return nil, fmt.Errorf(`attribute "region" is required in data_api configuration`)
	}
	return NewDataApiConfig(workgroupName, database, region, 1), nil
}
