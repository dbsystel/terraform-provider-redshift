package redshift

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	roleNameAttr = "name"
)

var (
	_ resource.Resource                = &roleResource{}
	_ resource.ResourceWithConfigure   = &roleResource{}
	_ resource.ResourceWithImportState = &roleResource{}
)

func newRoleResource() resource.Resource {
	return &roleResource{}
}

type roleResource struct {
	frameworkClient
}

type roleResourceModel struct {
	ID   types.String `tfsdk:"id"`
	Name types.String `tfsdk:"name"`
}

func (r *roleResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role"
}

func (r *roleResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Manages a Redshift role. Roles are named collections of privileges that can be granted to users, groups, or other roles.
Roles allow you to create a hierarchy of permissions, where a role can inherit privileges from other roles.

For more information, see [Redshift Roles Documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_roles-managing.html).
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The role ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			roleNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "The name of the role. Role names are case-insensitive and must be unique within the database.",
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
				},
			},
		},
	}
}

func (r *roleResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *roleResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *roleResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model roleResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	roleName := model.Name.ValueString()
	roleID, err := createRole(db, roleName)
	if err != nil {
		resp.Diagnostics.AddError("Unable to create the role", err.Error())
		return
	}

	model.ID = types.StringValue(roleID)
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func createRole(db *DBConnection, roleName string) (string, error) {
	tx, err := startTransaction(db.client)
	if err != nil {
		return "", err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("CREATE ROLE %s", pq.QuoteIdentifier(roleName))
	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return "", fmt.Errorf("could not create redshift role: %w", err)
	}

	// Query SVV_ROLES to get the role info (similar to how datashares use SVV_DATASHARES)
	// SVV_ROLES should have: role_name, role_owner, role_id
	var roleID string
	query = "SELECT role_id FROM SVV_ROLES WHERE role_name = $1"
	log.Printf("[DEBUG] %s, $1=%s\n", query, roleName)
	if err := tx.QueryRow(query, roleName).Scan(&roleID); err != nil {
		return "", fmt.Errorf("could not verify role creation for %q: %w", roleName, err)
	}

	if err = tx.Commit(); err != nil {
		return "", fmt.Errorf("could not commit transaction: %w", err)
	}

	return roleID, nil
}

func (r *roleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model roleResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	var roleName string
	query := "SELECT role_name FROM SVV_ROLES WHERE role_id = $1"
	log.Printf("[DEBUG] %s, $1=%s\n", query, model.ID.ValueString())

	if err := db.QueryRow(query, model.ID.ValueString()).Scan(&roleName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[WARN] Redshift Role (%s) not found", model.ID.ValueString())
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Unable to read the role", err.Error())
		return
	}

	model.Name = types.StringValue(roleName)
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func (r *roleResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state roleResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if plan.Name.ValueString() != state.Name.ValueString() {
		if err := renameRole(db, state.Name.ValueString(), plan.Name.ValueString()); err != nil {
			resp.Diagnostics.AddError("Unable to rename the role", err.Error())
			return
		}
	}

	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func renameRole(db *DBConnection, oldName, newName string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("ALTER ROLE %s RENAME TO %s",
		pq.QuoteIdentifier(oldName),
		pq.QuoteIdentifier(newName))
	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("error renaming role: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (r *roleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model roleResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if err := retryOnPQErrors(ctx, func() error { return deleteRole(db, model.ID.ValueString()) }); err != nil {
		resp.Diagnostics.AddError("Unable to delete the role", err.Error())
	}
}

func deleteRole(db *DBConnection, roleID string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	var roleName string
	query := "SELECT role_name FROM SVV_ROLES WHERE role_id = $1"
	log.Printf("[DEBUG] %s, $1=%s\n", query, roleID)
	if err := tx.QueryRow(query, roleID).Scan(&roleName); err != nil {
		return err
	}

	if roleName == "" {
		log.Printf("[WARN] Role with name %s does not exist.\n", roleID)
		return nil
	}

	query = fmt.Sprintf("DROP ROLE %s", pq.QuoteIdentifier(roleName))
	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("error dropping role: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}
