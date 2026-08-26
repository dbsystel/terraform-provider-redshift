package redshift

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = &schemaDataSource{}
	_ datasource.DataSourceWithConfigure = &schemaDataSource{}
)

func newSchemaDataSource() datasource.DataSource {
	return &schemaDataSource{}
}

type schemaDataSource struct {
	frameworkClient
}

type schemaDataSourceModel struct {
	ID             types.String `tfsdk:"id"`
	Name           types.String `tfsdk:"name"`
	Owner          types.String `tfsdk:"owner"`
	Quota          types.Int64  `tfsdk:"quota"`
	ExternalSchema types.List   `tfsdk:"external_schema"`
}

// The external schema is reported as a single computed object, since a data source has
// nothing to configure about it.
var (
	dataCatalogSourceAttributeTypes = map[string]attr.Type{
		"region":            types.StringType,
		"iam_role_arns":     types.ListType{ElemType: types.StringType},
		"catalog_role_arns": types.ListType{ElemType: types.StringType},
	}
	hiveMetastoreSourceAttributeTypes = map[string]attr.Type{
		"hostname":      types.StringType,
		"port":          types.Int64Type,
		"iam_role_arns": types.ListType{ElemType: types.StringType},
	}
	rdsPostgresSourceAttributeTypes = map[string]attr.Type{
		"hostname":      types.StringType,
		"port":          types.Int64Type,
		"schema":        types.StringType,
		"iam_role_arns": types.ListType{ElemType: types.StringType},
		"secret_arn":    types.StringType,
	}
	rdsMysqlSourceAttributeTypes = map[string]attr.Type{
		"hostname":      types.StringType,
		"port":          types.Int64Type,
		"iam_role_arns": types.ListType{ElemType: types.StringType},
		"secret_arn":    types.StringType,
	}
	redshiftSourceAttributeTypes = map[string]attr.Type{
		"schema": types.StringType,
	}
	externalSchemaAttributeTypes = map[string]attr.Type{
		"database_name":         types.StringType,
		"data_catalog_source":   types.ListType{ElemType: types.ObjectType{AttrTypes: dataCatalogSourceAttributeTypes}},
		"hive_metastore_source": types.ListType{ElemType: types.ObjectType{AttrTypes: hiveMetastoreSourceAttributeTypes}},
		"rds_postgres_source":   types.ListType{ElemType: types.ObjectType{AttrTypes: rdsPostgresSourceAttributeTypes}},
		"rds_mysql_source":      types.ListType{ElemType: types.ObjectType{AttrTypes: rdsMysqlSourceAttributeTypes}},
		"redshift_source":       types.ListType{ElemType: types.ObjectType{AttrTypes: redshiftSourceAttributeTypes}},
	}
)

func (d *schemaDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema"
}

func (d *schemaDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
A database contains one or more named schemas. Each schema in a database contains tables and other kinds of named objects. By default, a database has a single schema, which is named PUBLIC. You can use schemas to group database objects under a common name. Schemas are similar to file system directories, except that schemas cannot be nested.
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The schema ID.",
			},
			schemaNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the schema.",
			},
			schemaOwnerAttr: schema.StringAttribute{
				Computed:    true,
				Description: "Name of the schema owner.",
			},
			schemaQuotaAttr: schema.Int64Attribute{
				Computed:    true,
				Description: "The maximum amount of disk space that the specified schema can use. GB is the default unit of measurement.",
			},
			schemaExternalSchemaAttr: schema.ListNestedAttribute{
				Computed:    true,
				Description: "Configures the schema as an external schema. See https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html",
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"database_name": schema.StringAttribute{
							Computed:    true,
							Description: "The database where the external schema can be found",
						},
						"data_catalog_source": schema.ListNestedAttribute{
							Computed:    true,
							Description: "Configures the external schema from the AWS Glue Data Catalog",
							NestedObject: schema.NestedAttributeObject{
								Attributes: map[string]schema.Attribute{
									"region":            schema.StringAttribute{Computed: true, Description: "The AWS Region in which the database is located."},
									"iam_role_arns":     iamRoleArnsAttribute("The IAM roles the cluster uses for authentication and authorization."),
									"catalog_role_arns": iamRoleArnsAttribute("The IAM roles the cluster uses for authentication and authorization for the data catalog."),
								},
							},
						},
						"hive_metastore_source": schema.ListNestedAttribute{
							Computed:    true,
							Description: "Configures the external schema from a Hive Metastore.",
							NestedObject: schema.NestedAttributeObject{
								Attributes: map[string]schema.Attribute{
									"hostname":      schema.StringAttribute{Computed: true, Description: "The hostname of the hive metastore database."},
									"port":          schema.Int64Attribute{Computed: true, Description: "The port number of the hive metastore."},
									"iam_role_arns": iamRoleArnsAttribute("The IAM roles the cluster uses for authentication and authorization."),
								},
							},
						},
						"rds_postgres_source": schema.ListNestedAttribute{
							Computed:    true,
							Description: "Configures the external schema to reference data using a federated query to RDS POSTGRES or Aurora PostgreSQL.",
							NestedObject: schema.NestedAttributeObject{
								Attributes: map[string]schema.Attribute{
									"hostname":      schema.StringAttribute{Computed: true, Description: "The hostname of the head node of the PostgreSQL database replica set."},
									"port":          schema.Int64Attribute{Computed: true, Description: "The port number of the PostgreSQL database."},
									"schema":        schema.StringAttribute{Computed: true, Description: "The name of the PostgreSQL schema."},
									"iam_role_arns": iamRoleArnsAttribute("The IAM roles the cluster uses for authentication and authorization."),
									"secret_arn":    schema.StringAttribute{Computed: true, Description: "The ARN of the secret holding the database credentials."},
								},
							},
						},
						"rds_mysql_source": schema.ListNestedAttribute{
							Computed:    true,
							Description: "Configures the external schema to reference data using a federated query to RDS MYSQL or Aurora MySQL.",
							NestedObject: schema.NestedAttributeObject{
								Attributes: map[string]schema.Attribute{
									"hostname":      schema.StringAttribute{Computed: true, Description: "The hostname of the head node of the MySQL database replica set."},
									"port":          schema.Int64Attribute{Computed: true, Description: "The port number of the MySQL database."},
									"iam_role_arns": iamRoleArnsAttribute("The IAM roles the cluster uses for authentication and authorization."),
									"secret_arn":    schema.StringAttribute{Computed: true, Description: "The ARN of the secret holding the database credentials."},
								},
							},
						},
						"redshift_source": schema.ListNestedAttribute{
							Computed:    true,
							Description: "Configures the external schema to reference datashare database.",
							NestedObject: schema.NestedAttributeObject{
								Attributes: map[string]schema.Attribute{
									"schema": schema.StringAttribute{Computed: true, Description: "The name of the datashare schema."},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (d *schemaDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.configureDataSource(req, resp)
}

func (d *schemaDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var model schemaDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := d.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	schemaName := model.Name.ValueString()
	schemaID, schemaOwner, schemaType, err := readSchemaIDByName(db, schemaName)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the schema", err.Error())
		return
	}

	model.ID = types.StringValue(schemaID)
	model.Owner = types.StringValue(schemaOwner)
	model.ExternalSchema = types.ListNull(types.ObjectType{AttrTypes: externalSchemaAttributeTypes})

	switch schemaType {
	case "local":
		quota, err := readSchemaQuota(db, schemaName)
		if err != nil {
			resp.Diagnostics.AddError("Unable to read the schema quota", err.Error())
			return
		}
		model.Quota = types.Int64Value(quota)
	case "external":
		info, err := readExternalSchema(db, schemaID)
		if err != nil {
			resp.Diagnostics.AddError("Unable to read the external schema", err.Error())
			return
		}
		model.Quota = types.Int64Value(0)
		model.ExternalSchema = externalSchemaListValue(ctx, info, &resp.Diagnostics)
		if resp.Diagnostics.HasError() {
			return
		}
	default:
		resp.Diagnostics.AddError("Unable to read the schema", fmt.Sprintf("unsupported schema type %q: supported types are \"local\" and \"external\"", schemaType))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func externalSchemaListValue(ctx context.Context, info *externalSchemaInfo, diagnostics *diag.Diagnostics) types.List {
	iamRoleArns := listFromStrings(ctx, info.IamRoleArns, diagnostics)
	catalogRoleArns := listFromStrings(ctx, info.CatalogRoleArns, diagnostics)

	sources := map[string]attr.Value{
		"data_catalog_source":   types.ListNull(types.ObjectType{AttrTypes: dataCatalogSourceAttributeTypes}),
		"hive_metastore_source": types.ListNull(types.ObjectType{AttrTypes: hiveMetastoreSourceAttributeTypes}),
		"rds_postgres_source":   types.ListNull(types.ObjectType{AttrTypes: rdsPostgresSourceAttributeTypes}),
		"rds_mysql_source":      types.ListNull(types.ObjectType{AttrTypes: rdsMysqlSourceAttributeTypes}),
		"redshift_source":       types.ListNull(types.ObjectType{AttrTypes: redshiftSourceAttributeTypes}),
	}

	var (
		attributeTypes map[string]attr.Type
		attributes     map[string]attr.Value
	)
	switch info.SourceType {
	case "data_catalog_source":
		attributeTypes, attributes = dataCatalogSourceAttributeTypes, map[string]attr.Value{
			"region":            types.StringValue(info.Region),
			"iam_role_arns":     iamRoleArns,
			"catalog_role_arns": catalogRoleArns,
		}
	case "hive_metastore_source":
		attributeTypes, attributes = hiveMetastoreSourceAttributeTypes, map[string]attr.Value{
			"hostname":      types.StringValue(info.Hostname),
			"port":          types.Int64Value(info.Port),
			"iam_role_arns": iamRoleArns,
		}
	case "rds_postgres_source":
		attributeTypes, attributes = rdsPostgresSourceAttributeTypes, map[string]attr.Value{
			"hostname":      types.StringValue(info.Hostname),
			"port":          types.Int64Value(info.Port),
			"schema":        types.StringValue(info.SourceSchema),
			"iam_role_arns": iamRoleArns,
			"secret_arn":    types.StringValue(info.SecretArn),
		}
	case "rds_mysql_source":
		attributeTypes, attributes = rdsMysqlSourceAttributeTypes, map[string]attr.Value{
			"hostname":      types.StringValue(info.Hostname),
			"port":          types.Int64Value(info.Port),
			"iam_role_arns": iamRoleArns,
			"secret_arn":    types.StringValue(info.SecretArn),
		}
	case "redshift_source":
		attributeTypes, attributes = redshiftSourceAttributeTypes, map[string]attr.Value{
			"schema": types.StringValue(info.SourceSchema),
		}
	}

	source, diags := types.ObjectValue(attributeTypes, attributes)
	diagnostics.Append(diags...)
	sourceList, diags := types.ListValue(types.ObjectType{AttrTypes: attributeTypes}, []attr.Value{source})
	diagnostics.Append(diags...)
	sources[info.SourceType] = sourceList

	sources["database_name"] = types.StringValue(info.DatabaseName)

	external, diags := types.ObjectValue(externalSchemaAttributeTypes, sources)
	diagnostics.Append(diags...)

	list, diags := types.ListValue(types.ObjectType{AttrTypes: externalSchemaAttributeTypes}, []attr.Value{external})
	diagnostics.Append(diags...)
	return list
}

// iamRoleArnsAttribute describes one of the role lists an external schema is read with.
func iamRoleArnsAttribute(description string) schema.ListAttribute {
	return schema.ListAttribute{
		Computed:    true,
		ElementType: types.StringType,
		Description: description,
	}
}
