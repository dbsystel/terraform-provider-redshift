package redshift

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	datasharePrivilegeShareNameAttr = "share_name"
	datasharePrivilegeNamespaceAttr = "namespace"
	datasharePrivilegeAccountAttr   = "account"
	datasharePrivilegeShareDateAttr = "share_date"
)

var (
	_ resource.Resource              = &datasharePrivilegeResource{}
	_ resource.ResourceWithConfigure = &datasharePrivilegeResource{}
)

func newDatasharePrivilegeResource() resource.Resource {
	return &datasharePrivilegeResource{}
}

type datasharePrivilegeResource struct {
	frameworkClient
}

type datasharePrivilegeResourceModel struct {
	ID        types.String `tfsdk:"id"`
	ShareName types.String `tfsdk:"share_name"`
	Namespace types.String `tfsdk:"namespace"`
	Account   types.String `tfsdk:"account"`
	ShareDate types.String `tfsdk:"share_date"`
}

func (r *datasharePrivilegeResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_datashare_privilege"
}

func (r *datasharePrivilegeResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: fmt.Sprintf("Manages consumer permissions for [data sharing](https://docs.aws.amazon.com/redshift/latest/dg/datashare-overview.html).\n"+
			"\n"+
			"When managing datashare permissions between clusters in the same account, set the `%[1]s` to the consumer's namespace guid, and omit the `%[2]s`.\n"+
			"\n"+
			"When managing data share permissions across AWS accounts, set the `%[2]s` to the consumer's AWS account ID, and omit the `%[1]s`.\n"+
			"After creating the privilege through terraform, you will also need to [authorize the cross-account datashare through the AWS console](https://docs.aws.amazon.com/redshift/latest/dg/across-account.html) before consumer clusters can access it.\n"+
			"\n"+
			"Note: Data sharing is only supported on certain instance families, such as RA3.", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr),
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The datashare privilege ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			datasharePrivilegeShareNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the datashare",
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
					stringplanmodifier.RequiresReplace(),
				},
			},
			datasharePrivilegeNamespaceAttr: schema.StringAttribute{
				Optional:    true,
				Description: "Namespace (guid) of the consumer cluster, for sharing data within the same account. Either this or `account` must be specified.",
				Validators: []validator.String{
					stringvalidator.RegexMatches(uuidRegex, "Consumer namespace must be a guid"),
					// Exactly one of namespace or account must be set, which the SDK
					// resource had to express as a CustomizeDiff.
					stringvalidator.ExactlyOneOf(path.MatchRoot(datasharePrivilegeAccountAttr)),
				},
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
					stringplanmodifier.RequiresReplace(),
				},
			},
			datasharePrivilegeAccountAttr: schema.StringAttribute{
				Optional:    true,
				Description: "AWS account ID where the consumer cluster is located, for sharing data across accounts. Either this or `namespace` must be specified.",
				Validators: []validator.String{
					stringvalidator.RegexMatches(awsAccountIdRegexp, "AWS account id must be a 12-digit number"),
				},
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
					stringplanmodifier.RequiresReplace(),
				},
			},
			datasharePrivilegeShareDateAttr: schema.StringAttribute{
				Computed:    true,
				Description: "When the datashare permission was granted",
			},
		},
	}
}

func (r *datasharePrivilegeResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *datasharePrivilegeResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model datasharePrivilegeResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	shareName := model.ShareName.ValueString()
	query := fmt.Sprintf("GRANT USAGE ON DATASHARE %s TO ", pq.QuoteIdentifier(shareName))
	switch {
	case !model.Namespace.IsNull():
		query = fmt.Sprintf("%s NAMESPACE '%s'", query, pqQuoteLiteral(model.Namespace.ValueString()))
	case !model.Account.IsNull():
		query = fmt.Sprintf("%s ACCOUNT '%s'", query, pqQuoteLiteral(model.Account.ValueString()))
	default:
		resp.Diagnostics.AddError("Unable to grant the datashare privilege", fmt.Sprintf("either %s or %s is required", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr))
		return
	}

	log.Printf("[DEBUG] %s\n", query)
	if _, err := db.Exec(query); err != nil {
		resp.Diagnostics.AddError("Unable to grant the datashare privilege", err.Error())
		return
	}

	shareDate, err := readDatashareConsumerShareDate(db, model)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the datashare privilege", err.Error())
		return
	}

	model.ID = types.StringValue(generateDatasharePrivilegesID(model))
	model.ShareDate = types.StringValue(shareDate)
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func (r *datasharePrivilegeResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model datasharePrivilegeResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	shareDate, err := readDatashareConsumerShareDate(db, model)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the datashare privilege", err.Error())
		return
	}

	model.ShareDate = types.StringValue(shareDate)
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func readDatashareConsumerShareDate(db *DBConnection, model datasharePrivilegeResourceModel) (string, error) {
	var consumerColumn, consumer string
	switch {
	case !model.Namespace.IsNull():
		consumerColumn, consumer = "consumer_namespace", model.Namespace.ValueString()
	case !model.Account.IsNull():
		consumerColumn, consumer = "consumer_account", model.Account.ValueString()
	default:
		return "", fmt.Errorf("either %s or %s is required", datasharePrivilegeNamespaceAttr, datasharePrivilegeAccountAttr)
	}

	query := fmt.Sprintf(`SELECT
  REPLACE(TO_CHAR(share_date, 'YYYY-MM-DD HH24:MI:SS'), ' ', 'T') || 'Z'
FROM
  svv_datashare_consumers
WHERE
  share_name = $1
AND
  %s = $2`, consumerColumn)

	log.Printf("[DEBUG] %s\n", query)
	var shareDate string
	if err := db.QueryRow(query, model.ShareName.ValueString(), consumer).Scan(&shareDate); err != nil {
		return "", err
	}
	return shareDate, nil
}

// Update is never called: every argument requires replacement.
func (r *datasharePrivilegeResource) Update(_ context.Context, _ resource.UpdateRequest, _ *resource.UpdateResponse) {
}

func (r *datasharePrivilegeResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model datasharePrivilegeResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	query := fmt.Sprintf("REVOKE USAGE ON DATASHARE %s FROM", pq.QuoteIdentifier(model.ShareName.ValueString()))
	switch {
	case !model.Namespace.IsNull():
		query = fmt.Sprintf("%s NAMESPACE '%s'", query, pqQuoteLiteral(model.Namespace.ValueString()))
	case !model.Account.IsNull():
		query = fmt.Sprintf("%s ACCOUNT '%s'", query, model.Account.ValueString())
	}
	log.Printf("[DEBUG] %s\n", query)

	if _, err := db.Exec(query); err != nil {
		resp.Diagnostics.AddError("Unable to revoke the datashare privilege", err.Error())
	}
}

func generateDatasharePrivilegesID(model datasharePrivilegeResourceModel) string {
	source := []string{model.ShareName.ValueString()}
	switch {
	case !model.Namespace.IsNull():
		source = append(source, model.Namespace.ValueString())
	case !model.Account.IsNull():
		source = append(source, model.Account.ValueString())
	}
	return strings.Join(source, ".")
}
