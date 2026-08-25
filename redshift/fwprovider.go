package redshift

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"

	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/mapvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/setvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// frameworkProvider serves the parts of the provider that have been migrated to
// terraform-plugin-framework. It is muxed with the terraform-plugin-sdk/v2 provider in
// Provider(), which still serves everything else, so both must describe the exact same
// provider configuration: the mux rejects servers whose provider schemas differ.
type frameworkProvider struct{}

var _ provider.Provider = &frameworkProvider{}

func New() provider.Provider {
	return &frameworkProvider{}
}

type frameworkProviderModel struct {
	Host                 types.String                   `tfsdk:"host"`
	Username             types.String                   `tfsdk:"username"`
	Password             types.String                   `tfsdk:"password"`
	Port                 types.Int64                    `tfsdk:"port"`
	SSLMode              types.String                   `tfsdk:"sslmode"`
	Database             types.String                   `tfsdk:"database"`
	MaxConnections       types.Int64                    `tfsdk:"max_connections"`
	ConnectTimeout       types.Int64                    `tfsdk:"connect_timeout"`
	SessionParameters    types.Map                      `tfsdk:"session_parameters"`
	DataApi              []frameworkDataApiModel        `tfsdk:"data_api"`
	TemporaryCredentials []frameworkTemporaryCredsModel `tfsdk:"temporary_credentials"`
}

type frameworkDataApiModel struct {
	WorkgroupName     types.String `tfsdk:"workgroup_name"`
	ClusterIdentifier types.String `tfsdk:"cluster_identifier"`
	Username          types.String `tfsdk:"username"`
	Region            types.String `tfsdk:"region"`
}

type frameworkTemporaryCredsModel struct {
	ClusterIdentifier types.String               `tfsdk:"cluster_identifier"`
	Region            types.String               `tfsdk:"region"`
	AutoCreateUser    types.Bool                 `tfsdk:"auto_create_user"`
	DbGroups          types.Set                  `tfsdk:"db_groups"`
	DurationSeconds   types.Int64                `tfsdk:"duration_seconds"`
	AssumeRole        []frameworkAssumeRoleModel `tfsdk:"assume_role"`
}

type frameworkAssumeRoleModel struct {
	Arn         types.String `tfsdk:"arn"`
	ExternalID  types.String `tfsdk:"external_id"`
	SessionName types.String `tfsdk:"session_name"`
}

func (p *frameworkProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "redshift"
}

func (p *frameworkProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"host": schema.StringAttribute{
				Description: "Name of Redshift server address to connect to.",
				Optional:    true,
				Validators: []validator.String{
					stringvalidator.ConflictsWith(path.MatchRoot("data_api")),
				},
			},
			"username": schema.StringAttribute{
				Optional:    true,
				Description: "Redshift user name to connect as.",
			},
			"password": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "Password to be used if the Redshift server demands password authentication.",
				Validators: []validator.String{
					stringvalidator.ConflictsWith(path.MatchRoot("temporary_credentials")),
				},
			},
			"port": schema.Int64Attribute{
				Optional:    true,
				Description: "The Redshift port number to connect to at the server host.",
			},
			"sslmode": schema.StringAttribute{
				Optional:    true,
				Description: "This option determines whether or with what priority a secure SSL TCP/IP connection will be negotiated with the Redshift server. Valid values are `require` (default, always SSL, also skip verification), `verify-ca` (always SSL, verify that the certificate presented by the server was signed by a trusted CA), `verify-full` (always SSL, verify that the certification presented by the server was signed by a trusted CA and the server host name matches the one in the certificate), `disable` (no SSL).",
				Validators: []validator.String{
					stringvalidator.OneOf("require", "disable", "verify-ca", "verify-full"),
				},
			},
			"database": schema.StringAttribute{
				Optional:    true,
				Description: "The name of the database to connect to. The default is `redshift`.",
			},
			"max_connections": schema.Int64Attribute{
				Optional:    true,
				Description: "Maximum number of connections to establish to the database. Zero means unlimited.",
				Validators: []validator.Int64{
					int64validator.AtLeast(-1),
				},
			},
			"connect_timeout": schema.Int64Attribute{
				Optional:    true,
				Description: "Maximum wait for a connection to be established, in seconds. This covers the TCP connection, TLS negotiation and authentication handshake, but not the execution of individual statements. Zero applies no timeout, leaving the operating system to give up on the TCP connection in its own time. Unused when connecting via the Data API.",
				Validators: []validator.Int64{
					int64validator.AtLeast(0),
				},
			},
			"session_parameters": schema.MapAttribute{
				Optional:    true,
				ElementType: types.StringType,
				Description: "A map of session configuration parameters to apply to every connection the provider opens, sent using the libpq `options` connection parameter. Values are passed to Redshift unaltered and so use its own units. This map replaces the `PGOPTIONS` environment variable rather than merging with it. Parameter names may only contain lowercase letters, digits and underscores, and values only letters, digits and the characters `_.,:/@+-`. Cannot be combined with `data_api`, which opens a new session for each statement. See the [Amazon Redshift configuration reference](https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_ConfigurationRef.html) for the list of valid parameters.",
				Validators: []validator.Map{
					mapvalidator.ConflictsWith(path.MatchRoot("data_api")),
					sessionParametersValidator{},
				},
			},
		},
		Blocks: map[string]schema.Block{
			"data_api": schema.ListNestedBlock{
				Description: "Configuration for using the Redshift Data API. Supports both serverless workgroups and provisioned clusters.",
				Validators: []validator.List{
					listvalidator.SizeAtMost(1),
					listvalidator.ConflictsWith(
						path.MatchRoot("host"),
						path.MatchRoot("password"),
						path.MatchRoot("temporary_credentials"),
						path.MatchRoot("session_parameters"),
					),
				},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"workgroup_name": schema.StringAttribute{
							Optional:    true,
							Description: "The name of the Redshift Serverless workgroup to connect to.",
							Validators: []validator.String{
								stringvalidator.LengthBetween(3, 64),
								stringvalidator.RegexMatches(regexp.MustCompile("[a-z0-9-]+"), "must be lowercase alphanumeric or hyphen characters"),
								stringvalidator.ExactlyOneOf(path.MatchRelative().AtParent().AtName("cluster_identifier")),
							},
						},
						"cluster_identifier": schema.StringAttribute{
							Optional:    true,
							Description: "The identifier of the provisioned Redshift cluster to connect to.",
							Validators: []validator.String{
								stringvalidator.LengthBetween(1, 63),
							},
						},
						"username": schema.StringAttribute{
							Optional:    true,
							Description: "The database user to connect as. Required at apply time when cluster_identifier is set.",
							Validators: []validator.String{
								stringvalidator.LengthBetween(1, 128),
							},
						},
						"region": schema.StringAttribute{
							Optional:    true,
							Description: "The AWS region where the Redshift workgroup or cluster is located.",
						},
					},
				},
			},
			"temporary_credentials": schema.ListNestedBlock{
				Description: "Configuration for obtaining a temporary password using redshift:GetClusterCredentials",
				Validators: []validator.List{
					listvalidator.SizeAtMost(1),
					listvalidator.ConflictsWith(
						path.MatchRoot("password"),
						path.MatchRoot("data_api"),
					),
				},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"cluster_identifier": schema.StringAttribute{
							Required:    true,
							Description: "The unique identifier of the cluster that contains the database for which you are requesting credentials. This parameter is case sensitive.",
							Validators: []validator.String{
								stringvalidator.LengthBetween(1, 2147483647),
							},
						},
						"region": schema.StringAttribute{
							Optional:    true,
							Description: "The AWS region where the Redshift cluster is located.",
						},
						"auto_create_user": schema.BoolAttribute{
							Optional:    true,
							Description: "Create a database user with the name specified for the user if one does not exist.",
						},
						"db_groups": schema.SetAttribute{
							Optional:    true,
							ElementType: types.StringType,
							Description: "A list of the names of existing database groups that the user will join for the current session, in addition to any group memberships for an existing user. If not specified, a new user is added only to PUBLIC.",
							Validators: []validator.Set{
								setvalidator.ValueStringsAre(dbGroupValidators()...),
							},
						},
						"duration_seconds": schema.Int64Attribute{
							Optional:    true,
							Description: "The number of seconds until the returned temporary password expires.",
							Validators: []validator.Int64{
								int64validator.Between(900, 3600),
							},
						},
					},
					Blocks: map[string]schema.Block{
						"assume_role": schema.ListNestedBlock{
							Description: "Optional assume role data used to obtain temporary credentials",
							Validators: []validator.List{
								listvalidator.SizeAtMost(1),
							},
							NestedObject: schema.NestedBlockObject{
								Attributes: map[string]schema.Attribute{
									"arn": schema.StringAttribute{
										Required:    true,
										Description: "Amazon Resource Name of an IAM Role to assume prior to making API calls.",
									},
									"external_id": schema.StringAttribute{
										Optional:    true,
										Description: "A unique identifier that might be required when you assume a role in another account.",
										Validators: []validator.String{
											stringvalidator.LengthBetween(2, 1224),
											stringvalidator.RegexMatches(regexp.MustCompile(`[\w+=,.@:\/\-]*`), ""),
										},
									},
									"session_name": schema.StringAttribute{
										Optional:    true,
										Description: "An identifier for the assumed role session.",
										Validators: []validator.String{
											stringvalidator.LengthBetween(2, 64),
											stringvalidator.RegexMatches(regexp.MustCompile(`[\w+=,.@\-]*`), ""),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (p *frameworkProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var model frameworkProviderModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	settings, diags := model.settings(ctx)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	if len(settings.SessionParameters) > 0 && os.Getenv("PGOPTIONS") != "" {
		resp.Diagnostics.AddWarning(
			"PGOPTIONS is ignored",
			"The provider's `session_parameters` argument is set, which overrides the PGOPTIONS environment variable in its entirety. Move any settings you still need from PGOPTIONS into `session_parameters`.",
		)
	}

	config, err := settings.newConfig(temporaryCredentials)
	if err != nil {
		resp.Diagnostics.AddError("Unable to configure the Redshift provider", err.Error())
		return
	}

	log.Println("[DEBUG] creating database client")
	client := config.NewClient()
	log.Println("[DEBUG] created database client")
	resp.ResourceData = client
	resp.DataSourceData = client
}

// settings resolves the environment variable defaults that the SDK provider declares as
// DefaultFunc, which the framework has no equivalent for, and returns the provider
// arguments in their transport-agnostic form.
func (m *frameworkProviderModel) settings(ctx context.Context) (*providerSettings, diag.Diagnostics) {
	var diags diag.Diagnostics

	connectTimeoutRaw, _ := connectTimeoutDefault()
	settings := &providerSettings{
		Host:           stringWithEnvDefault(m.Host, "", "REDSHIFT_HOST"),
		Username:       stringWithEnvDefault(m.Username, "root", "REDSHIFT_USER"),
		Password:       stringWithEnvDefault(m.Password, "", "REDSHIFT_PASSWORD"),
		Port:           int64WithEnvDefault(m.Port, 5439, "REDSHIFT_PORT"),
		SSLMode:        stringWithEnvDefault(m.SSLMode, "require", "REDSHIFT_SSLMODE"),
		Database:       stringWithEnvDefault(m.Database, "redshift", "REDSHIFT_DATABASE"),
		MaxConnections: int64WithEnvDefault(m.MaxConnections, defaultProviderMaxOpenConnections),
		ConnectTimeout: int64WithEnvDefault(m.ConnectTimeout, connectTimeoutRaw.(int)),
	}

	if !m.SessionParameters.IsNull() {
		sessionParameters := map[string]string{}
		diags.Append(m.SessionParameters.ElementsAs(ctx, &sessionParameters, false)...)
		if diags.HasError() {
			return nil, diags
		}
		for name, value := range sessionParameters {
			if err := validateSessionParameter(name, value); err != nil {
				diags.AddAttributeError(path.Root("session_parameters").AtMapKey(name), "Invalid session parameter", err.Error())
				return nil, diags
			}
		}
		settings.SessionParameters = sessionParameters
	}

	if len(m.DataApi) > 0 {
		dataApi := m.DataApi[0]
		settings.DataApi = &dataApiSettings{
			WorkgroupName:     stringWithEnvDefault(dataApi.WorkgroupName, "", "REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME"),
			ClusterIdentifier: stringWithEnvDefault(dataApi.ClusterIdentifier, "", "REDSHIFT_DATA_API_CLUSTER_IDENTIFIER"),
			Username:          stringWithEnvDefault(dataApi.Username, "", "REDSHIFT_DATA_API_USERNAME"),
			Region:            stringWithEnvDefault(dataApi.Region, "", "AWS_REGION", "AWS_DEFAULT_REGION"),
		}
	}

	if len(m.TemporaryCredentials) > 0 {
		temporaryCredentials := m.TemporaryCredentials[0]
		resolved := &temporaryCredentialsSettings{
			ClusterIdentifier: temporaryCredentials.ClusterIdentifier.ValueString(),
			Region:            temporaryCredentials.Region.ValueString(),
			AutoCreateUser:    temporaryCredentials.AutoCreateUser.ValueBool(),
			DurationSeconds:   int(temporaryCredentials.DurationSeconds.ValueInt64()),
		}
		if !temporaryCredentials.DbGroups.IsNull() {
			diags.Append(temporaryCredentials.DbGroups.ElementsAs(ctx, &resolved.DbGroups, false)...)
			if diags.HasError() {
				return nil, diags
			}
		}
		if len(temporaryCredentials.AssumeRole) > 0 {
			assumeRole := temporaryCredentials.AssumeRole[0]
			resolved.AssumeRole = &assumeRoleSettings{
				Arn:         assumeRole.Arn.ValueString(),
				ExternalID:  assumeRole.ExternalID.ValueString(),
				SessionName: assumeRole.SessionName.ValueString(),
			}
		}
		settings.TemporaryCredentials = resolved
	}

	return settings, diags
}

func (p *frameworkProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		newRoleResource,
		newRoleGrantResource,
		newAssumeRoleGrantResource,
		newGroupMembershipResource,
		newDatasharePrivilegeResource,
		newDefaultPrivilegesResource,
	}
}

func (p *frameworkProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		newNamespaceDataSource,
		newUserDataSource,
		newGroupDataSource,
		newDatabaseDataSource,
	}
}

// stringWithEnvDefault mirrors schema.EnvDefaultFunc: the configured value wins, then the
// environment variables in order, then the built-in default.
func stringWithEnvDefault(value types.String, fallback string, envKeys ...string) string {
	if !value.IsNull() && !value.IsUnknown() {
		return value.ValueString()
	}
	for _, key := range envKeys {
		if fromEnv := os.Getenv(key); fromEnv != "" {
			return fromEnv
		}
	}
	return fallback
}

func int64WithEnvDefault(value types.Int64, fallback int, envKeys ...string) int {
	if !value.IsNull() && !value.IsUnknown() {
		return int(value.ValueInt64())
	}
	for _, key := range envKeys {
		fromEnv := os.Getenv(key)
		if fromEnv == "" {
			continue
		}
		parsed, err := strconv.Atoi(fromEnv)
		if err != nil {
			log.Printf("[WARN] ignoring %s value %q: not an integer", key, fromEnv)
			continue
		}
		return parsed
	}
	return fallback
}

// sessionParametersValidator reports invalid parameters during validation rather than
// waiting until the provider is configured, so that the diagnostic carries the attribute
// path and `terraform validate` rejects the configuration.
type sessionParametersValidator struct{}

func (v sessionParametersValidator) Description(_ context.Context) string {
	return "session parameter names and values must be valid"
}

func (v sessionParametersValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (v sessionParametersValidator) ValidateMap(ctx context.Context, req validator.MapRequest, resp *validator.MapResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	sessionParameters := map[string]types.String{}
	resp.Diagnostics.Append(req.ConfigValue.ElementsAs(ctx, &sessionParameters, false)...)
	if resp.Diagnostics.HasError() {
		return
	}
	for name, value := range sessionParameters {
		if value.IsUnknown() || value.IsNull() {
			continue
		}
		if err := validateSessionParameter(name, value.ValueString()); err != nil {
			resp.Diagnostics.AddAttributeError(req.Path.AtMapKey(name), "Invalid session parameter", err.Error())
		}
	}
}

func dbGroupValidators() []validator.String {
	return []validator.String{
		stringvalidator.LengthBetween(1, 64),
		stringvalidator.RegexMatches(dbGroupAcceptableCharacters, fmt.Sprintf("Must match regular expression %s", dbGroupAcceptableCharacters.String())),
		stringvalidator.RegexMatches(startsWithLetter, "First character must be a letter."),
		stringvalidator.RegexMatches(regexp.MustCompile(`^[^:/]*$`), "Must not contain ':' or '/'."),
		stringvalidator.NoneOfCaseInsensitive(reservedWords...),
	}
}
