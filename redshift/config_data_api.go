package redshift

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

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

func getConfigFromDataApiResourceData(d *schema.ResourceData, database string) (*Config, error) {
	workgroupName, workgroupNameOk := d.GetOk("data_api.0.workgroup_name")
	region, regionOk := d.GetOk("data_api.0.region")
	if !workgroupNameOk || !regionOk {
		return nil, fmt.Errorf("data_api configuration requires workgroup_name and region to be set")
	}
	return NewDataApiConfig(workgroupName.(string), database, region.(string), 1), nil
}
