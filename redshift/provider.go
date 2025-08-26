package redshift

import (
	"context"
	"log"
	"regexp"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

const (
	defaultProviderMaxOpenConnections                      = 20
	defaultTemporaryCredentialsAssumeRoleDurationInSeconds = 900
)

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"host": {
				Type:          schema.TypeString,
				Description:   "Name of Redshift server address to connect to.",
				Optional:      true,
				DefaultFunc:   schema.EnvDefaultFunc("REDSHIFT_HOST", nil),
				ConflictsWith: []string{"data_api"},
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_USER", "root"),
				Description: "Redshift user name to connect as.",
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_PASSWORD", nil),
				Description: "Password to be used if the Redshift server demands password authentication.",
				Sensitive:   true,
				ConflictsWith: []string{
					"temporary_credentials",
				},
			},
			"port": {
				Type:        schema.TypeInt,
				Description: "The Redshift port number to connect to at the server host.",
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_PORT", 5439),
			},
			"sslmode": {
				Type:        schema.TypeString,
				Description: "This option determines whether or with what priority a secure SSL TCP/IP connection will be negotiated with the Redshift server. Valid values are `require` (default, always SSL, also skip verification), `verify-ca` (always SSL, verify that the certificate presented by the server was signed by a trusted CA), `verify-full` (always SSL, verify that the certification presented by the server was signed by a trusted CA and the server host name matches the one in the certificate), `disable` (no SSL).",
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_SSLMODE", "require"),
				ValidateFunc: validation.StringInSlice([]string{
					"require",
					"disable",
					"verify-ca",
					"verify-full",
				}, false),
			},
			"database": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The name of the database to connect to. The default is `redshift`.",
				DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_DATABASE", "redshift"),
			},
			"max_connections": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      defaultProviderMaxOpenConnections,
				Description:  "Maximum number of connections to establish to the database. Zero means unlimited.",
				ValidateFunc: validation.IntAtLeast(-1),
			},
			"data_api": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Configuration for using the Redshift Data API. This can only be used for serverless Redshift clusters.",
				MaxItems:    1,
				ConflictsWith: []string{
					"host",
					"password",
				},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"workgroup_name": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "The name of the Redshift Serverless workgroup to connect to.",
							DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME", nil),
							// https://docs.aws.amazon.com/redshift-serverless/latest/APIReference/API_Workgroup.html#:~:text=Required%3A%20No-,workgroupName,-The%20name%20of
							ValidateFunc: validation.All(
								validation.StringLenBetween(3, 64),
								validation.StringMatch(regexp.MustCompile("[a-z0-9-]+"), "must be lowercase alphanumeric or hyphen characters"),
							),
						},
						"region": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "The AWS region where the Redshift Serverless workgroup is located. If not specified, the region will be determined from the AWS SDK configuration.",
							DefaultFunc: schema.MultiEnvDefaultFunc([]string{"AWS_REGION", "AWS_DEFAULT_REGION"}, nil),
						},
					},
				},
			},
			"temporary_credentials": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Configuration for obtaining a temporary password using redshift:GetClusterCredentials",
				MaxItems:    1,
				ConflictsWith: []string{
					"password",
					"data_api",
				},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"cluster_identifier": {
							Type:         schema.TypeString,
							Required:     true,
							Description:  "The unique identifier of the cluster that contains the database for which you are requesting credentials. This parameter is case sensitive.",
							ValidateFunc: validation.StringLenBetween(1, 2147483647),
						},
						"region": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "The AWS region where the Redshift cluster is located.",
						},
						"auto_create_user": {
							Type:        schema.TypeBool,
							Optional:    true,
							Description: "Create a database user with the name specified for the user if one does not exist.",
							Default:     false,
						},
						"db_groups": {
							Type:        schema.TypeSet,
							Set:         schema.HashString,
							Optional:    true,
							Description: "A list of the names of existing database groups that the user will join for the current session, in addition to any group memberships for an existing user. If not specified, a new user is added only to PUBLIC.",
							MaxItems:    2147483647,
							Elem: &schema.Schema{
								Type:         schema.TypeString,
								ValidateFunc: dbGroupValidate,
							},
						},
						"duration_seconds": {
							Type:         schema.TypeInt,
							Optional:     true,
							Description:  "The number of seconds until the returned temporary password expires.",
							ValidateFunc: validation.IntBetween(900, 3600),
						},
						"assume_role": assumeRoleSchema(),
					},
				},
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"redshift_user":                redshiftUser(),
			"redshift_group":               redshiftGroup(),
			"redshift_group_membership":    redshiftGroupMembership(),
			"redshift_schema":              redshiftSchema(),
			"redshift_default_privileges":  redshiftDefaultPrivileges(),
			"redshift_grant":               redshiftGrant(),
			"redshift_database":            redshiftDatabase(),
			"redshift_datashare":           redshiftDatashare(),
			"redshift_datashare_privilege": redshiftDatasharePrivilege(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"redshift_user":      dataSourceRedshiftUser(),
			"redshift_group":     dataSourceRedshiftGroup(),
			"redshift_schema":    dataSourceRedshiftSchema(),
			"redshift_database":  dataSourceRedshiftDatabase(),
			"redshift_namespace": dataSourceRedshiftNamespace(),
		},
		ConfigureContextFunc: providerConfigure,
	}
}

func providerConfigure(_ context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	cfg, err := getConfigFromResourceData(d, temporaryCredentials)
	if err != nil {
		return nil, diag.FromErr(err)
	}

	log.Println("[DEBUG] creating database client")
	client := cfg.NewClient()
	log.Println("[DEBUG] created database client")
	return client, nil
}

func getConfigFromResourceData(d *schema.ResourceData, temporaryCredentialsResolver temporaryCredentialsResolverFunc) (*Config, error) {
	database := d.Get("database").(string)
	maxConnections := d.Get("max_connections").(int)
	if _, useDataApi := d.GetOk("data_api"); useDataApi {
		return getConfigFromDataApiResourceData(d, database)
	}
	return getConfigFromPqResourceData(d, database, maxConnections, temporaryCredentialsResolver)
}

func assumeRoleSchema() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeList,
		Description: "Optional assume role data used to obtain temporary credentials",
		Optional:    true,
		MaxItems:    1,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"arn": {
					Type:        schema.TypeString,
					Required:    true,
					Description: "Amazon Resource Name of an IAM Role to assume prior to making API calls.",
				},
				"external_id": {
					Type:        schema.TypeString,
					Optional:    true,
					Description: "A unique identifier that might be required when you assume a role in another account.",
					ValidateFunc: validation.All(
						validation.StringLenBetween(2, 1224),
						validation.StringMatch(regexp.MustCompile(`[\w+=,.@:\/\-]*`), ""),
					),
				},
				"session_name": {
					Type:        schema.TypeString,
					Optional:    true,
					Description: "An identifier for the assumed role session.",
					ValidateFunc: validation.All(
						validation.StringLenBetween(2, 64),
						validation.StringMatch(regexp.MustCompile(`[\w+=,.@\-]*`), ""),
					),
				},
			},
		},
	}
}
