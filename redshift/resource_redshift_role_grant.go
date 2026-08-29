package redshift

import (
	"context"
	"database/sql"
	"errors"
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
	roleGrantRoleNameAttr    = "role_name"
	roleGrantGrantToTypeAttr = "grant_to_type"
	roleGrantGrantToNameAttr = "grant_to_name"
)

var (
	_ resource.Resource                = &roleGrantResource{}
	_ resource.ResourceWithConfigure   = &roleGrantResource{}
	_ resource.ResourceWithImportState = &roleGrantResource{}
)

func newRoleGrantResource() resource.Resource {
	return &roleGrantResource{}
}

type roleGrantResource struct {
	frameworkClient
}

type roleGrantResourceModel struct {
	ID          types.String `tfsdk:"id"`
	RoleName    types.String `tfsdk:"role_name"`
	GrantToType types.String `tfsdk:"grant_to_type"`
	GrantToName types.String `tfsdk:"grant_to_name"`
}

func (r *roleGrantResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role_grant"
}

func (r *roleGrantResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Grants a role to a user or another role. This allows hierarchical role-based access control in Redshift.

When a role is granted to another role, the recipient role inherits all privileges of the granted role. 
This enables role inheritance chains where permissions can be organized hierarchically.

For more information, see [GRANT documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html).
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The role grant ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			roleGrantRoleNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "The name of the role to grant.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			roleGrantGrantToTypeAttr: schema.StringAttribute{
				Required:    true,
				Description: "The type of principal to grant the role to. Valid values are: 'USER' or 'ROLE'.",
				Validators: []validator.String{
					stringvalidator.OneOf("USER", "ROLE"),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			roleGrantGrantToNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "The name of the user, or role to grant this role to.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *roleGrantResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *roleGrantResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *roleGrantResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model roleGrantResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	roleName, grantToType, grantToName := model.RoleName.ValueString(), model.GrantToType.ValueString(), model.GrantToName.ValueString()

	err := retryOnPQErrors(ctx, func() error { return grantRole(db, roleName, grantToType, grantToName) })
	if err != nil {
		resp.Diagnostics.AddError("Unable to grant the role", err.Error())
		return
	}

	model.ID = types.StringValue(generateRoleGrantID(roleName, grantToType, grantToName))
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func grantRole(db *DBConnection, roleName, grantToType, grantToName string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	// GRANT ROLE syntax in Redshift:
	// - For USER: GRANT ROLE role TO username (no USER keyword)
	// - For ROLE: GRANT ROLE role TO ROLE rolename (ROLE keyword required)
	var query string
	switch grantToType {
	case "USER":
		query = fmt.Sprintf("GRANT ROLE %s TO %s",
			pq.QuoteIdentifier(roleName),
			pq.QuoteIdentifier(grantToName))
	case "ROLE":
		query = fmt.Sprintf("GRANT ROLE %s TO ROLE %s",
			pq.QuoteIdentifier(roleName),
			pq.QuoteIdentifier(grantToName))
	default:
		return fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}

	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("could not grant role: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (r *roleGrantResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model roleGrantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	roleName, grantToType, grantToName := model.RoleName.ValueString(), model.GrantToType.ValueString(), model.GrantToName.ValueString()

	var query string
	switch grantToType {
	case "USER":
		// Check SVV_USER_GRANTS for role grants to users
		query = `
			SELECT 1
			FROM SVV_USER_GRANTS
			WHERE LOWER(role_name) = LOWER($1)
			AND LOWER(user_name) = LOWER($2)
		`
	case "ROLE":
		// Check SVV_ROLE_GRANTS for role grants to other roles
		// Note: role_name is the grantee (child), granted_role_name is the granted role (parent)
		query = `
			SELECT 1
			FROM SVV_ROLE_GRANTS
			WHERE LOWER(granted_role_name) = LOWER($1)
			AND LOWER(role_name) = LOWER($2)
		`
	default:
		resp.Diagnostics.AddError("Unable to read the role grant", fmt.Sprintf("unsupported grant_to_type: %s", grantToType))
		return
	}

	log.Printf("[DEBUG] %s, $1=%s, $2=%s\n", query, roleName, grantToName)

	var exists int
	if err := db.QueryRow(query, roleName, grantToName).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[WARN] Role grant %s to %s %s not found", roleName, grantToType, grantToName)
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Unable to read the role grant", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// Update is never called: every argument requires replacement.
func (r *roleGrantResource) Update(_ context.Context, _ resource.UpdateRequest, _ *resource.UpdateResponse) {
}

func (r *roleGrantResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model roleGrantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	roleName, grantToType, grantToName := model.RoleName.ValueString(), model.GrantToType.ValueString(), model.GrantToName.ValueString()

	err := retryOnPQErrors(ctx, func() error { return revokeRole(db, roleName, grantToType, grantToName) })
	if err != nil {
		resp.Diagnostics.AddError("Unable to revoke the role", err.Error())
	}
}

func revokeRole(db *DBConnection, roleName, grantToType, grantToName string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	// REVOKE ROLE syntax in Redshift:
	// - For USER: REVOKE ROLE role FROM username (no USER keyword)
	// - For ROLE: REVOKE ROLE role FROM ROLE rolename (ROLE keyword required)
	var query string
	switch grantToType {
	case "USER":
		query = fmt.Sprintf("REVOKE ROLE %s FROM %s",
			pq.QuoteIdentifier(roleName),
			pq.QuoteIdentifier(grantToName))
	case "ROLE":
		query = fmt.Sprintf("REVOKE ROLE %s FROM ROLE %s",
			pq.QuoteIdentifier(roleName),
			pq.QuoteIdentifier(grantToName))
	default:
		return fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}

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

func generateRoleGrantID(roleName, grantToType, grantToName string) string {
	return fmt.Sprintf("role:%s:%s:%s",
		strings.ToLower(roleName),
		strings.ToLower(grantToType),
		strings.ToLower(grantToName))
}
