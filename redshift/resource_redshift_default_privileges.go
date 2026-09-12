package redshift

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	defaultPrivilegesUserAttr       = "user"
	defaultPrivilegesGroupAttr      = "group"
	defaultPrivilegesRoleAttr       = "role"
	defaultPrivilegesOwnerAttr      = "owner"
	defaultPrivilegesSchemaAttr     = "schema"
	defaultPrivilegesPrivilegesAttr = "privileges"
	defaultPrivilegesObjectTypeAttr = "object_type"

	defaultPrivilegesAllSchemasID = 0
)

var defaultPrivilegesAllowedObjectTypes = []string{
	"table",
}

var (
	_ resource.Resource              = &defaultPrivilegesResource{}
	_ resource.ResourceWithConfigure = &defaultPrivilegesResource{}
)

func newDefaultPrivilegesResource() resource.Resource {
	return &defaultPrivilegesResource{}
}

type defaultPrivilegesResource struct {
	frameworkClient
}

type defaultPrivilegesResourceModel struct {
	ID         types.String `tfsdk:"id"`
	Schema     types.String `tfsdk:"schema"`
	Group      types.String `tfsdk:"group"`
	User       types.String `tfsdk:"user"`
	Role       types.String `tfsdk:"role"`
	Owner      types.String `tfsdk:"owner"`
	ObjectType types.String `tfsdk:"object_type"`
	Privileges types.Set    `tfsdk:"privileges"`
}

func (r *defaultPrivilegesResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_default_privileges"
}

func (r *defaultPrivilegesResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `Defines the default set of access privileges to be applied to objects that are created in the future by the specified user. By default, users can change only their own default access privileges. Only a superuser can specify default privileges for other users.`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The default privileges ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			defaultPrivilegesSchemaAttr: schema.StringAttribute{
				Optional:    true,
				Description: "If set, the specified default privileges are applied to new objects created in the specified schema. In this case, the user or user group that is the target of ALTER DEFAULT PRIVILEGES must have CREATE privilege for the specified schema. Default privileges that are specific to a schema are added to existing global default privileges. By default, default privileges are applied globally to the entire database.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			defaultPrivilegesGroupAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The name of the  group to which the specified default privileges are applied.",
				Validators: []validator.String{
					stringvalidator.ExactlyOneOf(
						path.MatchRoot(defaultPrivilegesUserAttr),
						path.MatchRoot(defaultPrivilegesRoleAttr),
					),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			defaultPrivilegesUserAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The name of the user to which the specified default privileges are applied.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			defaultPrivilegesRoleAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The name of the role to which the specified default privileges are applied.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			defaultPrivilegesOwnerAttr: schema.StringAttribute{
				Required:    true,
				Description: "The name of the user for which default privileges are defined. Only a superuser can specify default privileges for other users.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			defaultPrivilegesObjectTypeAttr: schema.StringAttribute{
				Required:    true,
				Description: "The Redshift object type to set the default privileges on (one of: " + strings.Join(defaultPrivilegesAllowedObjectTypes, ", ") + ").",
				Validators: []validator.String{
					stringvalidator.OneOf(defaultPrivilegesAllowedObjectTypes...),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			defaultPrivilegesPrivilegesAttr: schema.SetAttribute{
				Required:    true,
				ElementType: types.StringType,
				Description: "The list of privileges to apply as default privileges. See [ALTER DEFAULT PRIVILEGES command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_DEFAULT_PRIVILEGES.html) to see what privileges are available to which object type.",
				PlanModifiers: []planmodifier.Set{
					normalizeSet(strings.ToLower),
				},
			},
		},
	}
}

func (r *defaultPrivilegesResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *defaultPrivilegesResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model defaultPrivilegesResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	r.apply(ctx, model, &resp.Diagnostics, &resp.State)
}

func (r *defaultPrivilegesResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// Creating revokes everything first, so it doubles as the update.
	var model defaultPrivilegesResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	r.apply(ctx, model, &resp.Diagnostics, &resp.State)
}

func (r *defaultPrivilegesResource) apply(ctx context.Context, model defaultPrivilegesResourceModel, diagnostics *diag.Diagnostics, state *tfsdk.State) {
	db := r.connect(diagnostics)
	if db == nil {
		return
	}

	var privileges []string
	for _, privilege := range stringsFromSet(ctx, model.Privileges, diagnostics) {
		privileges = append(privileges, strings.ToUpper(privilege))
	}
	if diagnostics.HasError() {
		return
	}

	objectType := model.ObjectType.ValueString()
	if !validatePrivileges(privileges, objectType) {
		diagnostics.AddError("Invalid privileges", fmt.Sprintf("invalid privileges list %+v for object type %q", privileges, objectType))
		return
	}

	err := retryOnPQErrors(ctx, func() error { return applyDefaultPrivileges(db, model, privileges) })
	if err != nil {
		diagnostics.AddError("Unable to apply the default privileges", err.Error())
		return
	}

	model.ID = types.StringValue(generateDefaultPrivilegesID(model))
	diagnostics.Append(state.Set(ctx, model)...)
}

func applyDefaultPrivileges(db *DBConnection, model defaultPrivilegesResourceModel, privileges []string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if _, err := tx.Exec(createAlterDefaultsRevokeQuery(model)); err != nil {
		return err
	}

	if len(privileges) > 0 {
		if _, err := tx.Exec(createAlterDefaultsGrantQuery(model, privileges)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *defaultPrivilegesResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model defaultPrivilegesResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	privileges, err := readDefaultPrivileges(db, model)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the default privileges", err.Error())
		return
	}
	if privileges == nil {
		resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
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

// readDefaultPrivileges returns the privileges currently in effect, or nil for object
// types whose privileges the provider does not read back.
func readDefaultPrivileges(db *DBConnection, model defaultPrivilegesResourceModel) ([]string, error) {
	tx, err := startTransaction(db.client)
	if err != nil {
		return nil, err
	}
	defer deferredRollback(tx)

	ownerName := model.Owner.ValueString()
	log.Printf("[DEBUG] getting ID for owner %s\n", ownerName)
	ownerID, err := getUserIDFromName(tx, ownerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get user ID: %w", err)
	}

	var privileges []string
	switch strings.ToUpper(model.ObjectType.ValueString()) {
	case "TABLE":
		log.Println("[DEBUG] reading default privileges")
		if privileges, err = readTableDefaultPrivileges(tx, model, ownerID); err != nil {
			return nil, fmt.Errorf("failed to read table privileges: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	return privileges, nil
}

func readTableDefaultPrivileges(tx *sql.Tx, model defaultPrivilegesResourceModel, ownerID int) ([]string, error) {
	var tableSelect, tableUpdate, tableInsert, tableDelete, tableDrop, tableReferences, tableTruncate, tableAlter bool

	entityName, entityType, _ := defaultPrivilegesEntity(model)

	queryArgs := []interface{}{entityName, entityType, ownerID}
	schemaFilter := "AND schema_name IS NULL"
	if !model.Schema.IsNull() {
		schemaFilter = "AND schema_name = $4"
		queryArgs = append(queryArgs, model.Schema.ValueString())
	}

	query := fmt.Sprintf(`
		SELECT
			COALESCE(MAX(CASE WHEN privilege_type = 'SELECT' THEN 1 ELSE 0 END), 0) AS SELECT,
			COALESCE(MAX(CASE WHEN privilege_type = 'UPDATE' THEN 1 ELSE 0 END), 0) AS UPDATE,
			COALESCE(MAX(CASE WHEN privilege_type = 'INSERT' THEN 1 ELSE 0 END), 0) AS INSERT,
			COALESCE(MAX(CASE WHEN privilege_type = 'DELETE' THEN 1 ELSE 0 END), 0) AS DELETE,
			COALESCE(MAX(CASE WHEN privilege_type = 'DROP' THEN 1 ELSE 0 END), 0) AS DROP,
			COALESCE(MAX(CASE WHEN privilege_type = 'REFERENCES' THEN 1 ELSE 0 END), 0) AS REFERENCES,
			COALESCE(MAX(CASE WHEN privilege_type = 'TRUNCATE' THEN 1 ELSE 0 END), 0) AS TRUNCATE,
			COALESCE(MAX(CASE WHEN privilege_type = 'ALTER' THEN 1 ELSE 0 END), 0) AS ALTER
		FROM svv_default_privileges
		WHERE object_type = 'RELATION'
			AND grantee_name = $1
			AND grantee_type = $2
			AND owner_id = $3
			%s
		`, schemaFilter)

	if err := tx.QueryRow(query, queryArgs...).Scan(
		&tableSelect,
		&tableUpdate,
		&tableInsert,
		&tableDelete,
		&tableDrop,
		&tableReferences,
		&tableTruncate,
		&tableAlter); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("failed to collect privileges: %w", err)
	}

	privileges := []string{}
	appendIfTrue(tableSelect, "select", &privileges)
	appendIfTrue(tableUpdate, "update", &privileges)
	appendIfTrue(tableInsert, "insert", &privileges)
	appendIfTrue(tableDelete, "delete", &privileges)
	appendIfTrue(tableDrop, "drop", &privileges)
	appendIfTrue(tableReferences, "references", &privileges)
	appendIfTrue(tableTruncate, "truncate", &privileges)
	appendIfTrue(tableAlter, "alter", &privileges)

	log.Printf("[DEBUG] Collected privileges for entity %s %s: %v\n", entityType, entityName, privileges)

	return privileges, nil
}

func (r *defaultPrivilegesResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model defaultPrivilegesResourceModel
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

		if _, err := tx.Exec(createAlterDefaultsRevokeQuery(model)); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to revoke the default privileges", err.Error())
	}
}

// defaultPrivilegesEntity returns the grantee's name, its type as svv_default_privileges
// records it, and the keyword the ALTER DEFAULT PRIVILEGES statement needs in front of it.
func defaultPrivilegesEntity(model defaultPrivilegesResourceModel) (name, entityType, keyword string) {
	switch {
	case !model.Group.IsNull():
		return model.Group.ValueString(), "group", "GROUP"
	case !model.User.IsNull():
		return model.User.ValueString(), "user", ""
	case !model.Role.IsNull():
		return model.Role.ValueString(), "role", "ROLE"
	}
	return "", "", ""
}

func generateDefaultPrivilegesID(model defaultPrivilegesResourceModel) string {
	var entityName string
	switch {
	case !model.Group.IsNull():
		entityName = fmt.Sprintf("gn:%s", model.Group.ValueString())
	case !model.User.IsNull():
		entityName = fmt.Sprintf("un:%s", model.User.ValueString())
	case !model.Role.IsNull():
		entityName = fmt.Sprintf("rn:%s", model.Role.ValueString())
	}

	schemaName := "noschema"
	if !model.Schema.IsNull() {
		schemaName = fmt.Sprintf("sn:%s", model.Schema.ValueString())
	}

	return strings.Join([]string{
		entityName,
		schemaName,
		fmt.Sprintf("on:%s", model.Owner.ValueString()),
		fmt.Sprintf("ot:%s", model.ObjectType.ValueString()),
	}, "_")
}

func createAlterDefaultsGrantQuery(model defaultPrivilegesResourceModel, privileges []string) string {
	entityName, _, toWhomIndicator := defaultPrivilegesEntity(model)

	return fmt.Sprintf(
		"%s GRANT %s ON %sS TO %s %s",
		alterDefaultPrivilegesQuery(model),
		strings.Join(privileges, ","),
		strings.ToUpper(model.ObjectType.ValueString()),
		toWhomIndicator,
		pq.QuoteIdentifier(entityName),
	)
}

func createAlterDefaultsRevokeQuery(model defaultPrivilegesResourceModel) string {
	entityName, _, fromWhomIndicator := defaultPrivilegesEntity(model)

	return fmt.Sprintf(
		"%s REVOKE ALL PRIVILEGES ON %sS FROM %s %s",
		alterDefaultPrivilegesQuery(model),
		strings.ToUpper(model.ObjectType.ValueString()),
		fromWhomIndicator,
		pq.QuoteIdentifier(entityName),
	)
}

func alterDefaultPrivilegesQuery(model defaultPrivilegesResourceModel) string {
	query := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s", pq.QuoteIdentifier(model.Owner.ValueString()))
	if !model.Schema.IsNull() {
		query = fmt.Sprintf("%s IN SCHEMA %s", query, pq.QuoteIdentifier(model.Schema.ValueString()))
	}
	return query
}
