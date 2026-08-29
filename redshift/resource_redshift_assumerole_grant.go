package redshift

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/setvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	assumeRoleGrantRoleNameAttr    = "iam_role"
	assumeRoleGrantGrantToTypeAttr = "grant_to_type"
	assumeRoleGrantGrantToNameAttr = "grant_to_name"
	assumeRoleGrantPrivilegesAttr  = "privileges"
)

var (
	_ resource.Resource                = &assumeRoleGrantResource{}
	_ resource.ResourceWithConfigure   = &assumeRoleGrantResource{}
	_ resource.ResourceWithImportState = &assumeRoleGrantResource{}
)

func newAssumeRoleGrantResource() resource.Resource {
	return &assumeRoleGrantResource{}
}

type assumeRoleGrantResource struct {
	frameworkClient
}

type assumeRoleGrantResourceModel struct {
	ID          types.String `tfsdk:"id"`
	IamRole     types.String `tfsdk:"iam_role"`
	GrantToType types.String `tfsdk:"grant_to_type"`
	GrantToName types.String `tfsdk:"grant_to_name"`
	Privileges  types.Set    `tfsdk:"privileges"`
}

func (r *assumeRoleGrantResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_assumerole_grant"
}

func (r *assumeRoleGrantResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Grants the permission to use an IAM role to a user or a role.

For more information, see [GRANT documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html).
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The assume role grant ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			assumeRoleGrantRoleNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "The ARN of the role to be granted. 'default' and 'ALL' cannot be used in this resource.",
				Validators: []validator.String{
					stringvalidator.NoneOfCaseInsensitive("default", "all"),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			assumeRoleGrantGrantToTypeAttr: schema.StringAttribute{
				Required:    true,
				Description: "The type of principal to grant the role to. Valid values are: 'USER', 'ROLE'.",
				Validators: []validator.String{
					stringvalidator.OneOf("USER", "ROLE"),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			assumeRoleGrantGrantToNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "The name of the user, or role to grant this role to.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			assumeRoleGrantPrivilegesAttr: schema.SetAttribute{
				Required:    true,
				ElementType: types.StringType,
				Description: "The list of privileges to apply. See [GRANT command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html) to see what privileges are available. 'ALL' cannot be used in this resource.",
				Validators: []validator.Set{
					setvalidator.ValueStringsAre(
						stringvalidator.OneOfCaseInsensitive("copy", "unload", "external function", "create model"),
					),
				},
				PlanModifiers: []planmodifier.Set{
					normalizeSet(strings.ToLower),
					setplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *assumeRoleGrantResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *assumeRoleGrantResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *assumeRoleGrantResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model assumeRoleGrantResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	privileges := stringsFromSet(ctx, model.Privileges, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName, grantToType, grantToName := model.IamRole.ValueString(), model.GrantToType.ValueString(), model.GrantToName.ValueString()

	err := retryOnPQErrors(ctx, func() error {
		return grantAssumeRole(db, roleName, grantToType, grantToName, privileges)
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to grant assumerole", err.Error())
		return
	}

	model.ID = types.StringValue(generateAssumeRoleGrantID(roleName, strings.Join(privileges, ","), grantToType, grantToName))
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func grantAssumeRole(db *DBConnection, roleName, grantToType, grantToName string, privileges []string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("GRANT ASSUMEROLE ON %s", pq.QuoteLiteral(roleName))
	switch grantToType {
	case "USER":
		query = fmt.Sprintf("%s TO %s", query, pq.QuoteIdentifier(grantToName))
	case "ROLE":
		query = fmt.Sprintf("%s TO ROLE %s", query, pq.QuoteIdentifier(grantToName))
	default:
		return fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}
	query = fmt.Sprintf("%s FOR %s", query, strings.Join(privileges, ","))

	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("could not grant assumerole: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (r *assumeRoleGrantResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model assumeRoleGrantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	roleName, grantToType, grantToName := model.IamRole.ValueString(), model.GrantToType.ValueString(), model.GrantToName.ValueString()

	query := `
		SELECT
			COALESCE(MAX(CASE WHEN command_type = 'COPY' THEN 1 ELSE 0 END), 0),
			COALESCE(MAX(CASE WHEN command_type = 'UNLOAD' THEN 1 ELSE 0 END), 0),
			COALESCE(MAX(CASE WHEN command_type = 'EXFUNC' THEN 1 ELSE 0 END), 0),
			COALESCE(MAX(CASE WHEN command_type = 'CREATE MODEL' THEN 1 ELSE 0 END), 0)
		FROM SVV_IAM_PRIVILEGES
		WHERE iam_arn = $1
			AND identity_name = $2
			AND identity_type = LOWER($3)
		`

	log.Printf("[DEBUG] %s, $1=%s, $2=%s\n", query, roleName, grantToName)

	var copyPrivilege, unload, externalFunction, createModel bool
	err := retryOnPQErrors(ctx, func() error {
		return db.QueryRow(query, roleName, grantToName, grantToType).Scan(&copyPrivilege, &unload, &externalFunction, &createModel)
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the assume role grant", fmt.Sprintf("failed to collect privileges: %s", err))
		return
	}

	var privileges []string
	appendIfTrue(copyPrivilege, "copy", &privileges)
	appendIfTrue(unload, "unload", &privileges)
	appendIfTrue(externalFunction, "external function", &privileges)
	appendIfTrue(createModel, "create model", &privileges)

	if len(privileges) == 0 {
		log.Printf("[WARN] Assume role grant for %s to %s %s not found, removing from state", roleName, grantToType, grantToName)
		resp.State.RemoveResource(ctx)
		return
	}

	grantedPrivileges, diags := types.SetValueFrom(ctx, types.StringType, privileges)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	model.Privileges = grantedPrivileges
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// Update is never called: every argument requires replacement.
func (r *assumeRoleGrantResource) Update(_ context.Context, _ resource.UpdateRequest, _ *resource.UpdateResponse) {
}

func (r *assumeRoleGrantResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model assumeRoleGrantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	privileges := stringsFromSet(ctx, model.Privileges, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName, grantToType, grantToName := model.IamRole.ValueString(), model.GrantToType.ValueString(), model.GrantToName.ValueString()

	err := retryOnPQErrors(ctx, func() error {
		return revokeAssumeRole(db, roleName, grantToType, grantToName, privileges)
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to revoke assumerole", err.Error())
	}
}

func revokeAssumeRole(db *DBConnection, roleName, grantToType, grantToName string, privileges []string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("REVOKE ASSUMEROLE ON %s", pq.QuoteLiteral(roleName))
	switch grantToType {
	case "USER":
		query = fmt.Sprintf("%s FROM %s", query, pq.QuoteIdentifier(grantToName))
	case "ROLE":
		query = fmt.Sprintf("%s FROM ROLE %s", query, pq.QuoteIdentifier(grantToName))
	default:
		return fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}
	query = fmt.Sprintf("%s FOR %s", query, strings.Join(privileges, ","))

	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		// If the role or grantee doesn't exist, the grant is already gone
		if strings.Contains(err.Error(), "does not exist") {
			log.Printf("[WARN] Role or grantee does not exist, grant already removed: %v", err)
			// Still need to commit the transaction even if nothing was done
			if err = tx.Commit(); err != nil {
				return fmt.Errorf("could not commit transaction: %w", err)
			}
			return nil
		}
		return fmt.Errorf("could not revoke role: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func generateAssumeRoleGrantID(roleName, privilege, grantToType, grantToName string) string {
	return fmt.Sprintf("role;%s;%s;%s;%s",
		strings.ToLower(roleName),
		strings.ToLower(privilege),
		strings.ToLower(grantToType),
		strings.ToLower(grantToName))
}
