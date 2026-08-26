package redshift

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/boolvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	schemaNameAttr            = "name"
	schemaOwnerAttr           = "owner"
	schemaQuotaAttr           = "quota"
	schemaCascadeOnDeleteAttr = "cascade_on_delete"
	schemaExternalSchemaAttr  = "external_schema"

	// The quota is configured in GB and stored, like the cluster reports it, in MB.
	schemaQuotaMegabytesPerGigabyte = 1024
)

var (
	_ resource.Resource                = &schemaResource{}
	_ resource.ResourceWithConfigure   = &schemaResource{}
	_ resource.ResourceWithImportState = &schemaResource{}
)

func newSchemaResource() resource.Resource {
	return &schemaResource{}
}

type schemaResource struct {
	frameworkClient
}

type schemaResourceModel struct {
	ID              types.String          `tfsdk:"id"`
	Name            types.String          `tfsdk:"name"`
	Owner           types.String          `tfsdk:"owner"`
	Quota           types.Int64           `tfsdk:"quota"`
	CascadeOnDelete types.Bool            `tfsdk:"cascade_on_delete"`
	ExternalSchema  []externalSchemaModel `tfsdk:"external_schema"`
}

type externalSchemaModel struct {
	DatabaseName        types.String               `tfsdk:"database_name"`
	DataCatalogSource   []dataCatalogSourceModel   `tfsdk:"data_catalog_source"`
	HiveMetastoreSource []hiveMetastoreSourceModel `tfsdk:"hive_metastore_source"`
	RdsPostgresSource   []rdsPostgresSourceModel   `tfsdk:"rds_postgres_source"`
	RdsMysqlSource      []rdsMysqlSourceModel      `tfsdk:"rds_mysql_source"`
	RedshiftSource      []redshiftSourceModel      `tfsdk:"redshift_source"`
}

type dataCatalogSourceModel struct {
	Region                            types.String `tfsdk:"region"`
	IamRoleArns                       types.List   `tfsdk:"iam_role_arns"`
	CatalogRoleArns                   types.List   `tfsdk:"catalog_role_arns"`
	CreateExternalDatabaseIfNotExists types.Bool   `tfsdk:"create_external_database_if_not_exists"`
}

type hiveMetastoreSourceModel struct {
	Hostname    types.String `tfsdk:"hostname"`
	Port        types.Int64  `tfsdk:"port"`
	IamRoleArns types.List   `tfsdk:"iam_role_arns"`
}

type rdsPostgresSourceModel struct {
	Hostname    types.String `tfsdk:"hostname"`
	Port        types.Int64  `tfsdk:"port"`
	Schema      types.String `tfsdk:"schema"`
	IamRoleArns types.List   `tfsdk:"iam_role_arns"`
	SecretArn   types.String `tfsdk:"secret_arn"`
}

type rdsMysqlSourceModel struct {
	Hostname    types.String `tfsdk:"hostname"`
	Port        types.Int64  `tfsdk:"port"`
	IamRoleArns types.List   `tfsdk:"iam_role_arns"`
	SecretArn   types.String `tfsdk:"secret_arn"`
}

type redshiftSourceModel struct {
	Schema types.String `tfsdk:"schema"`
}

func (r *schemaResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema"
}

// sourceBlockValidators makes each external schema source block exclusive with the other
// four, the way the SDK resource did with ConflictsWith.
func sourceBlockValidators(others ...string) []validator.List {
	conflicts := make([]path.Expression, 0, len(others))
	for _, other := range others {
		conflicts = append(conflicts, path.MatchRelative().AtParent().AtName(other))
	}
	return []validator.List{
		listvalidator.SizeAtMost(1),
		listvalidator.ConflictsWith(conflicts...),
	}
}

func (r *schemaResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
A database contains one or more named schemas. Each schema in a database contains tables and other kinds of named objects. By default, a database has a single schema, which is named PUBLIC. You can use schemas to group database objects under a common name. Schemas are similar to file system directories, except that schemas cannot be nested.
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The schema ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			schemaNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the schema. The schema name can't be `PUBLIC`.",
				Validators: []validator.String{
					stringvalidator.NoneOfCaseInsensitive("public"),
				},
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
				},
			},
			schemaOwnerAttr: schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Name of the schema owner.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			schemaQuotaAttr: schema.Int64Attribute{
				Optional:    true,
				Computed:    true,
				Default:     int64default.StaticInt64(0),
				Description: "The maximum amount of disk space that the specified schema can use. GB is the default unit of measurement.",
				Validators: []validator.Int64{
					int64validator.AtLeast(0),
					int64validator.ConflictsWith(path.MatchRoot(schemaExternalSchemaAttr)),
				},
				PlanModifiers: []planmodifier.Int64{
					// State keeps the quota in MB, which is the unit the cluster reports.
					scaleInt64(schemaQuotaMegabytesPerGigabyte),
					int64planmodifier.UseStateForUnknown(),
				},
			},
			schemaCascadeOnDeleteAttr: schema.BoolAttribute{
				Optional:    true,
				Description: "Indicates to automatically drop all objects in the schema. The default action is TO NOT drop a schema if it contains any objects.",
				Validators: []validator.Bool{
					boolvalidator.ConflictsWith(path.MatchRoot(schemaExternalSchemaAttr)),
				},
			},
		},
		Blocks: map[string]schema.Block{
			schemaExternalSchemaAttr: schema.ListNestedBlock{
				Description: "Configures the schema as an external schema. See https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html",
				Validators: []validator.List{
					listvalidator.SizeAtMost(1),
				},
				PlanModifiers: []planmodifier.List{
					requiresReplaceIfListSizeChanged(),
				},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"database_name": schema.StringAttribute{
							Required:    true,
							Description: "The database where the external schema can be found",
							PlanModifiers: []planmodifier.String{
								stringplanmodifier.RequiresReplace(),
							},
						},
					},
					Blocks: map[string]schema.Block{
						"data_catalog_source": schema.ListNestedBlock{
							Description: "Configures the external schema from the AWS Glue Data Catalog",
							Validators:  sourceBlockValidators("hive_metastore_source", "rds_postgres_source", "rds_mysql_source", "redshift_source"),
							PlanModifiers: []planmodifier.List{
								requiresReplaceIfListSizeChanged(),
							},
							NestedObject: schema.NestedBlockObject{
								Attributes: map[string]schema.Attribute{
									"region": schema.StringAttribute{
										Optional:    true,
										Computed:    true,
										Description: "If the external database is defined in an Athena data catalog or the AWS Glue Data Catalog, the AWS Region in which the database is located. This parameter is required if the database is defined in an external Data Catalog.",
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.UseStateForUnknown(),
											stringplanmodifier.RequiresReplace(),
										},
									},
									"iam_role_arns": schema.ListAttribute{
										Required:    true,
										ElementType: types.StringType,
										Description: schemaIamRoleArnsCatalogDescription,
										Validators: []validator.List{
											listvalidator.SizeBetween(1, 10),
										},
										PlanModifiers: []planmodifier.List{
											listplanmodifier.RequiresReplace(),
										},
									},
									"catalog_role_arns": schema.ListAttribute{
										Optional:    true,
										Computed:    true,
										ElementType: types.StringType,
										Description: schemaCatalogRoleArnsDescription,
										Validators: []validator.List{
											listvalidator.SizeBetween(1, 10),
										},
										PlanModifiers: []planmodifier.List{
											listplanmodifier.UseStateForUnknown(),
											listplanmodifier.RequiresReplace(),
										},
									},
									"create_external_database_if_not_exists": schema.BoolAttribute{
										Optional:    true,
										Computed:    true,
										Default:     booldefault.StaticBool(false),
										Description: schemaCreateExternalDatabaseDescription,
										PlanModifiers: []planmodifier.Bool{
											// The flag only matters while the schema is
											// created and cannot be read back.
											ignoreChangesAfterCreate(),
										},
									},
								},
							},
						},
						"hive_metastore_source": schema.ListNestedBlock{
							Description: "Configures the external schema from a Hive Metastore.",
							Validators:  sourceBlockValidators("data_catalog_source", "rds_postgres_source", "rds_mysql_source", "redshift_source"),
							PlanModifiers: []planmodifier.List{
								requiresReplaceIfListSizeChanged(),
							},
							NestedObject: schema.NestedBlockObject{
								Attributes: map[string]schema.Attribute{
									"hostname": schema.StringAttribute{
										Required:    true,
										Description: "The hostname of the hive metastore database.",
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.RequiresReplace(),
										},
									},
									"port": schema.Int64Attribute{
										Optional:    true,
										Computed:    true,
										Default:     int64default.StaticInt64(9083),
										Description: "The port number of the hive metastore. The default port number is 9083.",
										Validators: []validator.Int64{
											int64validator.Between(1, 65535),
										},
										PlanModifiers: []planmodifier.Int64{
											int64planmodifier.RequiresReplace(),
										},
									},
									"iam_role_arns": schema.ListAttribute{
										Required:    true,
										ElementType: types.StringType,
										Description: schemaIamRoleArnsHiveDescription,
										Validators: []validator.List{
											listvalidator.SizeBetween(1, 10),
										},
										PlanModifiers: []planmodifier.List{
											listplanmodifier.RequiresReplace(),
										},
									},
								},
							},
						},
						"rds_postgres_source": schema.ListNestedBlock{
							Description: "Configures the external schema to reference data using a federated query to RDS POSTGRES or Aurora PostgreSQL.",
							Validators:  sourceBlockValidators("data_catalog_source", "hive_metastore_source", "rds_mysql_source", "redshift_source"),
							PlanModifiers: []planmodifier.List{
								requiresReplaceIfListSizeChanged(),
							},
							NestedObject: schema.NestedBlockObject{
								Attributes: map[string]schema.Attribute{
									"hostname": schema.StringAttribute{
										Required:    true,
										Description: "The hostname of the head node of the PostgreSQL database replica set.",
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.RequiresReplace(),
										},
									},
									"port": schema.Int64Attribute{
										Optional:    true,
										Computed:    true,
										Default:     int64default.StaticInt64(5432),
										Description: "The port number of the PostgreSQL database. The default port number is 5432.",
										Validators: []validator.Int64{
											int64validator.Between(1, 65535),
										},
										PlanModifiers: []planmodifier.Int64{
											int64planmodifier.RequiresReplace(),
										},
									},
									"schema": schema.StringAttribute{
										Optional:    true,
										Computed:    true,
										Default:     stringdefault.StaticString("public"),
										Description: "The name of the PostgreSQL schema. The default schema is 'public'",
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.RequiresReplace(),
										},
									},
									"iam_role_arns": schema.ListAttribute{
										Required:    true,
										ElementType: types.StringType,
										Description: schemaIamRoleArnsPostgresDescription,
										Validators: []validator.List{
											listvalidator.SizeBetween(1, 10),
										},
										PlanModifiers: []planmodifier.List{
											listplanmodifier.RequiresReplace(),
										},
									},
									"secret_arn": schema.StringAttribute{
										Required:    true,
										Description: schemaPostgresSecretArnDescription,
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.RequiresReplace(),
										},
									},
								},
							},
						},
						"rds_mysql_source": schema.ListNestedBlock{
							Description: "Configures the external schema to reference data using a federated query to RDS MYSQL or Aurora MySQL.",
							Validators:  sourceBlockValidators("data_catalog_source", "hive_metastore_source", "rds_postgres_source", "redshift_source"),
							PlanModifiers: []planmodifier.List{
								requiresReplaceIfListSizeChanged(),
							},
							NestedObject: schema.NestedBlockObject{
								Attributes: map[string]schema.Attribute{
									"hostname": schema.StringAttribute{
										Required:    true,
										Description: "The hostname of the head node of the MySQL database replica set.",
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.RequiresReplace(),
										},
									},
									"port": schema.Int64Attribute{
										Optional:    true,
										Computed:    true,
										Default:     int64default.StaticInt64(3306),
										Description: "The port number of the MySQL database. The default port number is 3306.",
										Validators: []validator.Int64{
											int64validator.Between(1, 65535),
										},
										PlanModifiers: []planmodifier.Int64{
											int64planmodifier.RequiresReplace(),
										},
									},
									"iam_role_arns": schema.ListAttribute{
										Required:    true,
										ElementType: types.StringType,
										Description: schemaIamRoleArnsMysqlDescription,
										Validators: []validator.List{
											listvalidator.SizeBetween(1, 10),
										},
										PlanModifiers: []planmodifier.List{
											listplanmodifier.RequiresReplace(),
										},
									},
									"secret_arn": schema.StringAttribute{
										Required:    true,
										Description: schemaMysqlSecretArnDescription,
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.RequiresReplace(),
										},
									},
								},
							},
						},
						"redshift_source": schema.ListNestedBlock{
							Description: "Configures the external schema to reference datashare database.",
							Validators:  sourceBlockValidators("data_catalog_source", "hive_metastore_source", "rds_postgres_source", "rds_mysql_source"),
							PlanModifiers: []planmodifier.List{
								requiresReplaceIfListSizeChanged(),
							},
							NestedObject: schema.NestedBlockObject{
								Attributes: map[string]schema.Attribute{
									"schema": schema.StringAttribute{
										Optional:    true,
										Computed:    true,
										Default:     stringdefault.StaticString("public"),
										Description: "The name of the datashare schema. The default schema is 'public'.",
										PlanModifiers: []planmodifier.String{
											stringplanmodifier.RequiresReplace(),
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

func (r *schemaResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *schemaResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

const (
	schemaIamRoleArnsCatalogDescription = `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
  If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
  For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

  When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
  If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

  To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`

	schemaIamRoleArnsHiveDescription = `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
	If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

  When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
	If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

  To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`

	schemaIamRoleArnsPostgresDescription = `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
	If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

  When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
	If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

  To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`

	schemaIamRoleArnsMysqlDescription = `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization.
	As a minimum, the IAM roles must have permission to perform a LIST operation on the Amazon S3 bucket to be accessed and a GET operation on the Amazon S3 objects the bucket contains.
	If the external database is defined in an Amazon Athena data catalog or the AWS Glue Data Catalog, the IAM role must have permission to access Athena unless catalog_role is specified.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

  When you attach a role to your cluster, your cluster can assume that role to access Amazon S3, Athena, and AWS Glue on your behalf.
	If a role attached to your cluster doesn't have access to the necessary resources, you can chain another role, possibly belonging to another account.
	Your cluster then temporarily assumes the chained role to access the data. You can also grant cross-account access by chaining roles.
	You can chain a maximum of 10 roles. Each role in the chain assumes the next role in the chain, until the cluster assumes the role at the end of chain.

  To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`

	schemaCatalogRoleArnsDescription = `The Amazon Resource Name (ARN) for the IAM roles that your cluster uses for authentication and authorization for the data catalog.
	If this is not specified, Amazon Redshift uses the specified iam_role_arns. The catalog role must have permission to access the Data Catalog in AWS Glue or Athena.
	For more information, see https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html.

  To chain roles, you establish a trust relationship between the roles. A role that assumes another role must have a permissions policy that allows it to assume the specified role.
	In turn, the role that passes permissions must have a trust policy that allows it to pass its permissions to another role.
	For more information, see https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-chaining-roles`

	schemaCreateExternalDatabaseDescription = `When enabled, creates an external database with the name specified by the database argument,
	if the specified external database doesn't exist. If the specified external database exists, the command makes no changes.
	In this case, the command returns a message that the external database exists, rather than terminating with an error.

  To use create_external_database_if_not_exists with a Data Catalog enabled for AWS Lake Formation, you need CREATE_DATABASE permission on the Data Catalog.`

	schemaPostgresSecretArnDescription = `The Amazon Resource Name (ARN) of a supported PostgreSQL database engine secret created using AWS Secrets Manager.
	For information about how to create and retrieve an ARN for a secret, see https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_create-basic-secret.html
	and https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_retrieve-secret.html in the AWS Secrets Manager User Guide.`

	schemaMysqlSecretArnDescription = `The Amazon Resource Name (ARN) of a supported MySQL database engine secret created using AWS Secrets Manager.
	For information about how to create and retrieve an ARN for a secret, see https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_create-basic-secret.html
	and https://docs.aws.amazon.com/secretsmanager/latest/userguide/manage_retrieve-secret.html in the AWS Secrets Manager User Guide.`
)

func (r *schemaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model schemaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	schemaID, err := createSchema(ctx, db, model, &resp.Diagnostics)
	if err != nil {
		resp.Diagnostics.AddError("Unable to create the schema", err.Error())
		return
	}
	if resp.Diagnostics.HasError() {
		return
	}

	model.ID = types.StringValue(schemaID)
	if !r.refresh(ctx, db, &model, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func createSchema(ctx context.Context, db *DBConnection, model schemaResourceModel, diagnostics *diag.Diagnostics) (string, error) {
	tx, err := startTransaction(db.client)
	if err != nil {
		return "", err
	}
	defer deferredRollback(tx)

	schemaName := model.Name.ValueString()

	if len(model.ExternalSchema) > 0 {
		query, err := createExternalSchemaQuery(ctx, model, diagnostics)
		if err != nil {
			return "", err
		}
		if diagnostics.HasError() {
			return "", nil
		}

		log.Printf("[DEBUG] creating external schema: %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return "", err
		}

		if owner := model.Owner.ValueString(); owner != "" && !model.Owner.IsUnknown() {
			query = fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(owner))
			log.Printf("[DEBUG] setting schema owner: %s\n", query)
			if _, err := tx.Exec(query); err != nil {
				return "", err
			}
		}
	} else {
		var createOpts []string
		if owner := model.Owner.ValueString(); owner != "" && !model.Owner.IsUnknown() {
			createOpts = append(createOpts, fmt.Sprintf("AUTHORIZATION %s", pq.QuoteIdentifier(owner)))
		}
		createOpts = append(createOpts, schemaQuotaClause(model))

		query := fmt.Sprintf("CREATE SCHEMA %s %s", pq.QuoteIdentifier(schemaName), strings.Join(createOpts, " "))
		if _, err := tx.Exec(query); err != nil {
			return "", err
		}
	}

	var schemaOID string
	if err := tx.QueryRow("SELECT oid FROM pg_namespace WHERE nspname = $1", strings.ToLower(schemaName)).Scan(&schemaOID); err != nil {
		return "", err
	}

	if err = tx.Commit(); err != nil {
		return "", fmt.Errorf("could not commit transaction: %w", err)
	}

	return schemaOID, nil
}

// schemaQuotaClause renders the quota, which state keeps in MB, back into the GB the
// CREATE SCHEMA statement expects.
func schemaQuotaClause(model schemaResourceModel) string {
	if quota := model.Quota.ValueInt64() / schemaQuotaMegabytesPerGigabyte; quota > 0 {
		return fmt.Sprintf("QUOTA %d GB", quota)
	}
	return "QUOTA UNLIMITED"
}

func createExternalSchemaQuery(ctx context.Context, model schemaResourceModel, diagnostics *diag.Diagnostics) (string, error) {
	external := model.ExternalSchema[0]
	sourceDbName := external.DatabaseName.ValueString()

	var configQuery string
	switch {
	case len(external.DataCatalogSource) > 0:
		source := external.DataCatalogSource[0]
		configQuery = fmt.Sprintf("FROM DATA CATALOG DATABASE '%s'", pqQuoteLiteral(sourceDbName))
		if region := source.Region.ValueString(); region != "" && !source.Region.IsUnknown() {
			configQuery = fmt.Sprintf("%s REGION '%s'", configQuery, pqQuoteLiteral(region))
		}
		configQuery = fmt.Sprintf("%s IAM_ROLE '%s'", configQuery, pqQuoteLiteral(strings.Join(stringsFromList(ctx, source.IamRoleArns, diagnostics), ",")))
		if catalogRoleArns := stringsFromList(ctx, source.CatalogRoleArns, diagnostics); len(catalogRoleArns) > 0 {
			configQuery = fmt.Sprintf("%s CATALOG_ROLE '%s'", configQuery, pqQuoteLiteral(strings.Join(catalogRoleArns, ",")))
		}
		if source.CreateExternalDatabaseIfNotExists.ValueBool() {
			configQuery = fmt.Sprintf("%s CREATE EXTERNAL DATABASE IF NOT EXISTS", configQuery)
		}
	case len(external.HiveMetastoreSource) > 0:
		source := external.HiveMetastoreSource[0]
		configQuery = fmt.Sprintf("FROM HIVE METASTORE DATABASE '%s'", pqQuoteLiteral(sourceDbName))
		configQuery = fmt.Sprintf("%s URI '%s'", configQuery, pqQuoteLiteral(source.Hostname.ValueString()))
		if port := source.Port.ValueInt64(); port != 0 {
			configQuery = fmt.Sprintf("%s PORT %d", configQuery, port)
		}
		configQuery = fmt.Sprintf("%s IAM_ROLE '%s'", configQuery, pqQuoteLiteral(strings.Join(stringsFromList(ctx, source.IamRoleArns, diagnostics), ",")))
	case len(external.RdsPostgresSource) > 0:
		source := external.RdsPostgresSource[0]
		configQuery = fmt.Sprintf("FROM POSTGRES DATABASE '%s'", pqQuoteLiteral(sourceDbName))
		if sourceSchema := source.Schema.ValueString(); sourceSchema != "" {
			configQuery = fmt.Sprintf("%s SCHEMA '%s'", configQuery, pqQuoteLiteral(sourceSchema))
		}
		configQuery = fmt.Sprintf("%s URI '%s'", configQuery, pqQuoteLiteral(source.Hostname.ValueString()))
		if port := source.Port.ValueInt64(); port != 0 {
			configQuery = fmt.Sprintf("%s PORT %d", configQuery, port)
		}
		configQuery = fmt.Sprintf("%s IAM_ROLE '%s'", configQuery, pqQuoteLiteral(strings.Join(stringsFromList(ctx, source.IamRoleArns, diagnostics), ",")))
		configQuery = fmt.Sprintf("%s SECRET_ARN '%s'", configQuery, pqQuoteLiteral(source.SecretArn.ValueString()))
	case len(external.RdsMysqlSource) > 0:
		source := external.RdsMysqlSource[0]
		configQuery = fmt.Sprintf("FROM MYSQL DATABASE '%s'", pqQuoteLiteral(sourceDbName))
		configQuery = fmt.Sprintf("%s URI '%s'", configQuery, pqQuoteLiteral(source.Hostname.ValueString()))
		if port := source.Port.ValueInt64(); port != 0 {
			configQuery = fmt.Sprintf("%s PORT %d", configQuery, port)
		}
		configQuery = fmt.Sprintf("%s IAM_ROLE '%s'", configQuery, pqQuoteLiteral(strings.Join(stringsFromList(ctx, source.IamRoleArns, diagnostics), ",")))
		configQuery = fmt.Sprintf("%s SECRET_ARN '%s'", configQuery, pqQuoteLiteral(source.SecretArn.ValueString()))
	case len(external.RedshiftSource) > 0:
		source := external.RedshiftSource[0]
		configQuery = fmt.Sprintf("FROM REDSHIFT DATABASE '%s'", pqQuoteLiteral(sourceDbName))
		if sourceSchema := source.Schema.ValueString(); sourceSchema != "" {
			configQuery = fmt.Sprintf("%s SCHEMA '%s'", configQuery, pqQuoteLiteral(sourceSchema))
		}
	default:
		return "", fmt.Errorf("can't create external schema: no source configuration found")
	}

	return fmt.Sprintf("CREATE EXTERNAL SCHEMA %s %s", pq.QuoteIdentifier(model.Name.ValueString()), configQuery), nil
}

func (r *schemaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model schemaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if !r.refresh(ctx, db, &model, &resp.Diagnostics) {
		return
	}
	if model.ID.IsNull() {
		resp.State.RemoveResource(ctx)
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// refresh fills the model from the cluster. It clears the ID when the schema is gone,
// and reports whether the caller may go on.
func (r *schemaResource) refresh(ctx context.Context, db *DBConnection, model *schemaResourceModel, diagnostics *diag.Diagnostics) bool {
	schemaName, schemaOwner, schemaType, err := readSchemaBasics(db, model.ID.ValueString())
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[WARN] Redshift Schema (%s) not found", model.ID.ValueString())
			model.ID = types.StringNull()
			return true
		}
		diagnostics.AddError("Unable to read the schema", err.Error())
		return false
	}

	model.Name = types.StringValue(schemaName)
	model.Owner = types.StringValue(schemaOwner)

	switch schemaType {
	case "local":
		quota, err := readSchemaQuota(db, schemaName)
		if err != nil {
			diagnostics.AddError("Unable to read the schema quota", err.Error())
			return false
		}
		model.Quota = types.Int64Value(quota)
		model.ExternalSchema = nil
	case "external":
		info, err := readExternalSchema(db, model.ID.ValueString())
		if err != nil {
			diagnostics.AddError("Unable to read the external schema", err.Error())
			return false
		}
		model.Quota = types.Int64Value(0)
		model.ExternalSchema = []externalSchemaModel{externalSchemaModelFromInfo(ctx, info, model.ExternalSchema, diagnostics)}
		if diagnostics.HasError() {
			return false
		}
	default:
		diagnostics.AddError("Unable to read the schema", fmt.Sprintf("unsupported schema type %q: supported types are \"local\" and \"external\"", schemaType))
		return false
	}

	return true
}

// externalSchemaModelFromInfo shapes what the cluster reported into the resource model.
// create_external_database_if_not_exists is carried over from the previous state: the
// cluster does not report it.
func externalSchemaModelFromInfo(ctx context.Context, info *externalSchemaInfo, previous []externalSchemaModel, diagnostics *diag.Diagnostics) externalSchemaModel {
	external := externalSchemaModel{DatabaseName: types.StringValue(info.DatabaseName)}

	iamRoleArns := listFromStrings(ctx, info.IamRoleArns, diagnostics)
	catalogRoleArns := listFromStrings(ctx, info.CatalogRoleArns, diagnostics)

	switch info.SourceType {
	case "data_catalog_source":
		createDatabase := types.BoolValue(false)
		if len(previous) > 0 && len(previous[0].DataCatalogSource) > 0 {
			createDatabase = previous[0].DataCatalogSource[0].CreateExternalDatabaseIfNotExists
		}
		external.DataCatalogSource = []dataCatalogSourceModel{{
			Region:                            types.StringValue(info.Region),
			IamRoleArns:                       iamRoleArns,
			CatalogRoleArns:                   catalogRoleArns,
			CreateExternalDatabaseIfNotExists: createDatabase,
		}}
	case "hive_metastore_source":
		external.HiveMetastoreSource = []hiveMetastoreSourceModel{{
			Hostname:    types.StringValue(info.Hostname),
			Port:        types.Int64Value(info.Port),
			IamRoleArns: iamRoleArns,
		}}
	case "rds_postgres_source":
		external.RdsPostgresSource = []rdsPostgresSourceModel{{
			Hostname:    types.StringValue(info.Hostname),
			Port:        types.Int64Value(info.Port),
			Schema:      types.StringValue(info.SourceSchema),
			IamRoleArns: iamRoleArns,
			SecretArn:   types.StringValue(info.SecretArn),
		}}
	case "rds_mysql_source":
		external.RdsMysqlSource = []rdsMysqlSourceModel{{
			Hostname:    types.StringValue(info.Hostname),
			Port:        types.Int64Value(info.Port),
			IamRoleArns: iamRoleArns,
			SecretArn:   types.StringValue(info.SecretArn),
		}}
	case "redshift_source":
		external.RedshiftSource = []redshiftSourceModel{{
			Schema: types.StringValue(info.SourceSchema),
		}}
	}

	return external
}

func (r *schemaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state schemaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if err := updateSchema(db, plan, state); err != nil {
		resp.Diagnostics.AddError("Unable to update the schema", err.Error())
		return
	}

	plan.ID = state.ID
	if !r.refresh(ctx, db, &plan, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func updateSchema(db *DBConnection, plan, state schemaResourceModel) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	schemaName := plan.Name.ValueString()

	if schemaName != state.Name.ValueString() {
		if schemaName == "" {
			return fmt.Errorf("error setting schema name to an empty string")
		}
		query := fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s", pq.QuoteIdentifier(state.Name.ValueString()), pq.QuoteIdentifier(schemaName))
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating schema NAME: %w", err)
		}
	}

	if !plan.Owner.IsUnknown() && plan.Owner.ValueString() != state.Owner.ValueString() {
		query := fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(plan.Owner.ValueString()))
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}

	if plan.Quota.ValueInt64() != state.Quota.ValueInt64() {
		quotaValue := "UNLIMITED"
		if quota := plan.Quota.ValueInt64() / schemaQuotaMegabytesPerGigabyte; quota > 0 {
			quotaValue = fmt.Sprintf("%d GB", quota)
		}
		if _, err := tx.Exec(fmt.Sprintf("ALTER SCHEMA %s QUOTA %s", pq.QuoteIdentifier(schemaName), quotaValue)); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (r *schemaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model schemaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	err := retryOnPQErrors(ctx, func() error {
		tx, err := startTransaction(db.client)
		if err != nil {
			return err
		}
		defer deferredRollback(tx)

		cascadeOrRestrict := "RESTRICT"
		if model.CascadeOnDelete.ValueBool() {
			cascadeOrRestrict = "CASCADE"
		}

		query := fmt.Sprintf("DROP SCHEMA %s %s", pq.QuoteIdentifier(model.Name.ValueString()), cascadeOrRestrict)
		if _, err := tx.Exec(query); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to delete the schema", err.Error())
	}
}
