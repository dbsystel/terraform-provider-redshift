package redshift

import (
	"context"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = &databaseDataSource{}
	_ datasource.DataSourceWithConfigure = &databaseDataSource{}
)

func newDatabaseDataSource() datasource.DataSource {
	return &databaseDataSource{}
}

type databaseDataSource struct {
	frameworkClient
}

type databaseDataSourceModel struct {
	ID              types.String `tfsdk:"id"`
	Name            types.String `tfsdk:"name"`
	Owner           types.String `tfsdk:"owner"`
	ConnectionLimit types.Int64  `tfsdk:"connection_limit"`
	DatashareSource types.List   `tfsdk:"datashare_source"`
}

// datashareSourceAttributeTypes describes one element of the datashare_source list.
var datashareSourceAttributeTypes = map[string]attr.Type{
	databaseDatashareSourceShareNameAttr: types.StringType,
	databaseDatashareSourceNamespaceAttr: types.StringType,
	databaseDatashareSourceAccountAttr:   types.StringType,
}

func (d *databaseDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

func (d *databaseDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `Fetches information about a Redshift database.`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The database ID.",
			},
			databaseNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the database",
			},
			databaseOwnerAttr: schema.StringAttribute{
				Computed:    true,
				Description: "Owner of the database, usually the user who created it",
			},
			databaseConnLimitAttr: schema.Int64Attribute{
				Computed:    true,
				Description: "The maximum number of concurrent connections that can be made to this database. A value of -1 means no limit.",
			},
			databaseDatashareSourceAttr: schema.ListNestedAttribute{
				Computed:    true,
				Description: "Configuration for a database created from a redshift datashare.",
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						databaseDatashareSourceShareNameAttr: schema.StringAttribute{
							Computed:    true,
							Description: "The name of the datashare on the producer cluster",
						},
						databaseDatashareSourceNamespaceAttr: schema.StringAttribute{
							Computed:    true,
							Description: "The namespace (guid) of the producer cluster",
						},
						databaseDatashareSourceAccountAttr: schema.StringAttribute{
							Computed:    true,
							Description: "The AWS account ID of the producer cluster.",
						},
					},
				},
			},
		},
	}
}

func (d *databaseDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.configureDataSource(req, resp)
}

func (d *databaseDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var model databaseDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := d.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	var id, owner, connLimit, databaseType, shareName, producerAccount, producerNamespace string

	err := db.QueryRow(`SELECT
  pg_database_info.datid,
  TRIM(pg_user_info.usename),
  COALESCE(pg_database_info.datconnlimit::text, 'UNLIMITED'),
	svv_redshift_databases.database_type,
  TRIM(COALESCE(svv_datashares.share_name, '')),
  TRIM(COALESCE(svv_datashares.producer_account, '')),
  TRIM(COALESCE(svv_datashares.producer_namespace, ''))
FROM
  svv_redshift_databases
LEFT JOIN pg_database_info
  ON svv_redshift_databases.database_name=pg_database_info.datname
LEFT JOIN pg_user_info
  ON pg_user_info.usesysid = svv_redshift_databases.database_owner
LEFT JOIN svv_datashares
	ON (svv_redshift_databases.database_name = svv_datashares.consumer_database AND svv_redshift_databases.database_type = 'shared' AND svv_datashares.share_type = 'INBOUND')
WHERE svv_redshift_databases.database_name = $1
	`, strings.ToLower(model.Name.ValueString())).Scan(&id, &owner, &connLimit, &databaseType, &shareName, &producerAccount, &producerNamespace)

	if err != nil {
		resp.Diagnostics.AddError("Unable to read the database", err.Error())
		return
	}

	connLimitNumber := -1
	if connLimit != "UNLIMITED" {
		if connLimitNumber, err = strconv.Atoi(connLimit); err != nil {
			resp.Diagnostics.AddError("Unable to read the database connection limit", err.Error())
			return
		}
	}

	datashareSources := []attr.Value{}
	if databaseType == "shared" {
		source, diags := types.ObjectValue(datashareSourceAttributeTypes, map[string]attr.Value{
			databaseDatashareSourceShareNameAttr: types.StringValue(shareName),
			databaseDatashareSourceNamespaceAttr: types.StringValue(producerNamespace),
			databaseDatashareSourceAccountAttr:   types.StringValue(producerAccount),
		})
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		datashareSources = append(datashareSources, source)
	}

	datashareSource, diags := types.ListValue(types.ObjectType{AttrTypes: datashareSourceAttributeTypes}, datashareSources)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	model.ID = types.StringValue(id)
	model.Owner = types.StringValue(owner)
	model.ConnectionLimit = types.Int64Value(int64(connLimitNumber))
	model.DatashareSource = datashareSource

	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}
