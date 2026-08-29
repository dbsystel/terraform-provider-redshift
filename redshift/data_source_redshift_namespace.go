package redshift

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = &namespaceDataSource{}
	_ datasource.DataSourceWithConfigure = &namespaceDataSource{}
)

func newNamespaceDataSource() datasource.DataSource {
	return &namespaceDataSource{}
}

type namespaceDataSource struct {
	frameworkClient
}

type namespaceDataSourceModel struct {
	ID types.String `tfsdk:"id"`
}

func (d *namespaceDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace"
}

func (d *namespaceDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `Gets the cluster namespace (unique ID) of the Amazon Redshift cluster.`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The cluster namespace.",
			},
		},
	}
}

func (d *namespaceDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.configureDataSource(req, resp)
}

func (d *namespaceDataSource) Read(ctx context.Context, _ datasource.ReadRequest, resp *datasource.ReadResponse) {
	db := d.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	var namespace string
	if err := db.QueryRow("SELECT CURRENT_NAMESPACE").Scan(&namespace); err != nil {
		resp.Diagnostics.AddError("Unable to read the cluster namespace", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, namespaceDataSourceModel{ID: types.StringValue(namespace)})...)
}
