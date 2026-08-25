package redshift

import (
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
)

// frameworkClient is embedded by every framework resource and data source. It holds the
// client handed out by the provider's Configure and opens connections from it, which is
// what ResourceFunc does for the parts still served by the SDK provider.
type frameworkClient struct {
	client *Client
}

func (c *frameworkClient) configureDataSource(req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	client, ok := req.ProviderData.(*Client)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data", "The provider did not hand out a Redshift client.")
		return
	}
	c.client = client
}

func (c *frameworkClient) configureResource(req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	client, ok := req.ProviderData.(*Client)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data", "The provider did not hand out a Redshift client.")
		return
	}
	c.client = client
}

// connect opens a connection, reporting a failure as a diagnostic. It returns nil when the
// connection could not be established, in which case the caller must return.
func (c *frameworkClient) connect(diagnostics *diag.Diagnostics) *DBConnection {
	db, err := c.client.Connect()
	if err != nil {
		diagnostics.AddError("Unable to connect to Redshift", err.Error())
		return nil
	}
	return db
}
