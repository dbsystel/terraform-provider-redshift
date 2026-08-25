package redshift

import (
	"context"
	"log"
	"os"
	"regexp"
	"strconv"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

const (
	defaultProviderMaxOpenConnections                      = 20
	defaultTemporaryCredentialsAssumeRoleDurationInSeconds = 900
	defaultConnectTimeoutInSeconds                         = 180
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
			// connect_timeout deliberately declares no ConflictsWith. Its DefaultFunc always
			// returns a value, which the SDK treats as the argument being set, so a conflict
			// would fire for every data_api user. Like port and sslmode, it is simply unused
			// by the Data API transport.
			"connect_timeout": {
				Type:         schema.TypeInt,
				Optional:     true,
				DefaultFunc:  connectTimeoutDefault,
				Description:  "Maximum wait for a connection to be established, in seconds. This covers the TCP connection, TLS negotiation and authentication handshake, but not the execution of individual statements. Zero applies no timeout, leaving the operating system to give up on the TCP connection in its own time. Unused when connecting via the Data API.",
				ValidateFunc: validation.IntAtLeast(0),
			},
			"session_parameters": {
				Type:             schema.TypeMap,
				Optional:         true,
				Elem:             &schema.Schema{Type: schema.TypeString},
				Description:      "A map of session configuration parameters to apply to every connection the provider opens, sent using the libpq `options` connection parameter. Values are passed to Redshift unaltered and so use its own units. This map replaces the `PGOPTIONS` environment variable rather than merging with it. Parameter names may only contain lowercase letters, digits and underscores, and values only letters, digits and the characters `_.,:/@+-`. Cannot be combined with `data_api`, which opens a new session for each statement. See the [Amazon Redshift configuration reference](https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_ConfigurationRef.html) for the list of valid parameters.",
				ConflictsWith:    []string{"data_api"},
				ValidateDiagFunc: validateSessionParameters,
			},
			"data_api": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Configuration for using the Redshift Data API. Supports both serverless workgroups and provisioned clusters.",
				MaxItems:    1,
				ConflictsWith: []string{
					"host",
					"password",
					"temporary_credentials",
					"session_parameters",
				},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"workgroup_name": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "The name of the Redshift Serverless workgroup to connect to.",
							DefaultFunc: schema.EnvDefaultFunc("REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME", nil),
							ValidateFunc: validation.All(
								validation.StringLenBetween(3, 64),
								validation.StringMatch(regexp.MustCompile("[a-z0-9-]+"), "must be lowercase alphanumeric or hyphen characters"),
							),
							ExactlyOneOf: []string{"data_api.0.workgroup_name", "data_api.0.cluster_identifier"},
						},
						"cluster_identifier": {
							Type:         schema.TypeString,
							Optional:     true,
							Description:  "The identifier of the provisioned Redshift cluster to connect to.",
							DefaultFunc:  schema.EnvDefaultFunc("REDSHIFT_DATA_API_CLUSTER_IDENTIFIER", nil),
							ValidateFunc: validation.StringLenBetween(1, 63),
							ExactlyOneOf: []string{"data_api.0.workgroup_name", "data_api.0.cluster_identifier"},
						},
						"username": {
							Type:         schema.TypeString,
							Optional:     true,
							Description:  "The database user to connect as. Required at apply time when cluster_identifier is set.",
							DefaultFunc:  schema.EnvDefaultFunc("REDSHIFT_DATA_API_USERNAME", nil),
							ValidateFunc: validation.StringLenBetween(1, 128),
						},
						// region is Optional rather than Required so that the schema does
						// not depend on the environment: the SDK downgrades a Required
						// argument to Optional whenever its DefaultFunc returns a value.
						// A missing region is reported when the provider is configured.
						"region": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "The AWS region where the Redshift workgroup or cluster is located.",
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
			"redshift_assumerole_grant":    redshiftAssumeRoleGrant(),
			"redshift_user":                redshiftUser(),
			"redshift_group":               redshiftGroup(),
			"redshift_group_membership":    redshiftGroupMembership(),
			"redshift_role":                redshiftRole(),
			"redshift_role_grant":          redshiftRoleGrant(),
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

// connectTimeoutDefault resolves the default from PGCONNECT_TIMEOUT, the variable libpq
// itself reads. A value that is not a non-negative integer falls back to the built-in
// default rather than failing: the argument is also present for Data API configurations,
// which never open a libpq connection and must not be broken by an unrelated variable in
// the environment. lib/pq reports the bad value, naming the variable, if a libpq
// connection is actually made.
func connectTimeoutDefault() (interface{}, error) {
	raw := os.Getenv("PGCONNECT_TIMEOUT")
	if raw == "" {
		return defaultConnectTimeoutInSeconds, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		log.Printf("[WARN] ignoring PGCONNECT_TIMEOUT value %q: not a non-negative integer", raw)
		return defaultConnectTimeoutInSeconds, nil
	}
	return seconds, nil
}

func providerConfigure(_ context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	cfg, err := getConfigFromResourceData(d, temporaryCredentials)
	if err != nil {
		return nil, diag.FromErr(err)
	}

	diags := warnOnShadowedPGOptions(d)

	log.Println("[DEBUG] creating database client")
	client := cfg.NewClient()
	log.Println("[DEBUG] created database client")
	return client, diags
}

// warnOnShadowedPGOptions reports that PGOPTIONS is being discarded. Session parameters
// are sent as the libpq `options` connection parameter, which takes precedence over the
// environment variable of the same meaning, so a user setting both silently loses the
// settings from their environment.
func warnOnShadowedPGOptions(d *schema.ResourceData) diag.Diagnostics {
	sessionParameters, ok := d.GetOk("session_parameters")
	if !ok || len(sessionParameters.(map[string]interface{})) == 0 {
		return nil
	}
	if os.Getenv("PGOPTIONS") == "" {
		return nil
	}
	return diag.Diagnostics{{
		Severity: diag.Warning,
		Summary:  "PGOPTIONS is ignored",
		Detail:   "The provider's `session_parameters` argument is set, which overrides the PGOPTIONS environment variable in its entirety. Move any settings you still need from PGOPTIONS into `session_parameters`.",
	}}
}

// settingsFromResourceData reads the provider arguments into a transport-agnostic
// struct, so that everything downstream is free of the SDK's resource data.
func settingsFromResourceData(d *schema.ResourceData) (*providerSettings, error) {
	sessionParameters, err := sessionParametersFromResourceData(d)
	if err != nil {
		return nil, err
	}

	settings := &providerSettings{
		Host:              d.Get("host").(string),
		Username:          d.Get("username").(string),
		Password:          d.Get("password").(string),
		Port:              d.Get("port").(int),
		SSLMode:           d.Get("sslmode").(string),
		Database:          d.Get("database").(string),
		MaxConnections:    d.Get("max_connections").(int),
		ConnectTimeout:    d.Get("connect_timeout").(int),
		SessionParameters: sessionParameters,
	}

	if _, ok := d.GetOk("data_api"); ok {
		settings.DataApi = &dataApiSettings{
			WorkgroupName:     d.Get("data_api.0.workgroup_name").(string),
			ClusterIdentifier: d.Get("data_api.0.cluster_identifier").(string),
			Username:          d.Get("data_api.0.username").(string),
			Region:            d.Get("data_api.0.region").(string),
		}
	}

	if _, ok := d.GetOk("temporary_credentials"); ok {
		temporaryCredentialsSettings := &temporaryCredentialsSettings{
			ClusterIdentifier: d.Get("temporary_credentials.0.cluster_identifier").(string),
			Region:            d.Get("temporary_credentials.0.region").(string),
			AutoCreateUser:    d.Get("temporary_credentials.0.auto_create_user").(bool),
			DurationSeconds:   d.Get("temporary_credentials.0.duration_seconds").(int),
		}
		if dbGroups, ok := d.GetOk("temporary_credentials.0.db_groups"); ok {
			for _, group := range dbGroups.(*schema.Set).List() {
				temporaryCredentialsSettings.DbGroups = append(temporaryCredentialsSettings.DbGroups, group.(string))
			}
		}
		if _, ok := d.GetOk("temporary_credentials.0.assume_role"); ok {
			temporaryCredentialsSettings.AssumeRole = &assumeRoleSettings{
				Arn:         d.Get("temporary_credentials.0.assume_role.0.arn").(string),
				ExternalID:  d.Get("temporary_credentials.0.assume_role.0.external_id").(string),
				SessionName: d.Get("temporary_credentials.0.assume_role.0.session_name").(string),
			}
		}
		settings.TemporaryCredentials = temporaryCredentialsSettings
	}

	return settings, nil
}

func getConfigFromResourceData(d *schema.ResourceData, temporaryCredentialsResolver temporaryCredentialsResolverFunc) (*Config, error) {
	settings, err := settingsFromResourceData(d)
	if err != nil {
		return nil, err
	}
	return settings.newConfig(temporaryCredentialsResolver)
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
