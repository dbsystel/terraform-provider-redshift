package redshift

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

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
	groupNameAttr  = "name"
	groupUsersAttr = "users"
)

var (
	_ resource.Resource                = &groupResource{}
	_ resource.ResourceWithConfigure   = &groupResource{}
	_ resource.ResourceWithImportState = &groupResource{}
)

func newGroupResource() resource.Resource {
	return &groupResource{}
}

type groupResource struct {
	frameworkClient
}

type groupResourceModel struct {
	ID    types.String `tfsdk:"id"`
	Name  types.String `tfsdk:"name"`
	Users types.Set    `tfsdk:"users"`
}

func (r *groupResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_group"
}

func (r *groupResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Groups are collections of users who are all granted whatever privileges are associated with the group. You can use groups to assign privileges by role. For example, you can create different groups for sales, administration, and support and give the users in each group the appropriate access to the data they require for their work. You can grant or revoke privileges at the group level, and those changes will apply to all members of the group, except for superusers.
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The group ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			groupNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the user group. Group names beginning with two underscores are reserved for Amazon Redshift internal use.",
				Validators: []validator.String{
					regexDoesNotMatch(regexp.MustCompile("^__.*"), "Group names beginning with two underscores are reserved for Amazon Redshift internal use"),
				},
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
				},
			},
			groupUsersAttr: schema.SetAttribute{
				Optional:    true,
				ElementType: types.StringType,
				Description: "List of the user names to add to the group",
			},
		},
	}
}

func (r *groupResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *groupResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *groupResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model groupResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	userNames := stringsFromSet(ctx, model.Users, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	groupID, err := createGroup(db, model.Name.ValueString(), userNames)
	if err != nil {
		resp.Diagnostics.AddError("Unable to create the group", err.Error())
		return
	}

	model.ID = types.StringValue(groupID)
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func createGroup(db *DBConnection, groupName string, userNames []string) (string, error) {
	tx, err := startTransaction(db.client)
	if err != nil {
		return "", err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("CREATE GROUP %s", pq.QuoteIdentifier(groupName))
	if len(userNames) > 0 {
		var usernamesSafe []string
		for _, name := range userNames {
			usernamesSafe = append(usernamesSafe, pq.QuoteIdentifier(name))
		}
		query = fmt.Sprintf("%s WITH USER %s", query, strings.Join(usernamesSafe, ", "))
	}

	if _, err := tx.Exec(query); err != nil {
		return "", fmt.Errorf("could not create redshift group: %w", err)
	}

	var groSysID string
	if err := tx.QueryRow("SELECT grosysid FROM pg_group WHERE groname = $1", strings.ToLower(groupName)).Scan(&groSysID); err != nil {
		return "", fmt.Errorf("could not get redshift group id for %q: %w", groupName, err)
	}

	if err = tx.Commit(); err != nil {
		return "", fmt.Errorf("could not commit transaction: %w", err)
	}

	return groSysID, nil
}

func (r *groupResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model groupResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	groupID := model.ID.ValueString()
	groupName, groupUsers, err := readGroup(db, groupID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Unable to read the group", err.Error())
		return
	}

	model.Name = types.StringValue(groupName)
	// An unmanaged group has no members; leaving the attribute unset in that case keeps
	// a configuration that omits it from planning a change on every run.
	if len(groupUsers) > 0 || !model.Users.IsNull() {
		users, diags := types.SetValueFrom(ctx, types.StringType, groupUsers)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		model.Users = users
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func readGroup(db *DBConnection, groupID string) (string, []string, error) {
	var (
		groupName  string
		groupUsers []string
	)

	query := `SELECT groname, u.usename FROM pg_user_info u, pg_group g WHERE g.grosysid = $1 AND u.usesysid = ANY(g.grolist);`
	rows, err := db.Query(query, groupID)
	if err != nil {
		return "", nil, err
	}
	for rows.Next() {
		var userName string
		if err = rows.Scan(&groupName, &userName); err != nil {
			_ = rows.Close()
			return "", nil, fmt.Errorf("could not read group members for group id %q: %w", groupID, err)
		}
		groupUsers = append(groupUsers, userName)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return "", nil, fmt.Errorf("could not read group members for group id %q: %w", groupID, err)
	}
	if err = rows.Close(); err != nil {
		return "", nil, fmt.Errorf("could not close group members rows for group id %q: %w", groupID, err)
	}

	if len(groupUsers) == 0 {
		// no users found so the group name could not be fetched, we have to query for the name
		query = `SELECT groname FROM pg_group WHERE grosysid = $1;`
		if err := db.QueryRow(query, groupID).Scan(&groupName); err != nil {
			return "", nil, err
		}
	}

	return groupName, groupUsers, nil
}

func (r *groupResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state groupResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	oldUserNames := stringsFromSet(ctx, state.Users, &resp.Diagnostics)
	newUserNames := stringsFromSet(ctx, plan.Users, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := updateGroup(db, state.Name.ValueString(), plan.Name.ValueString(), oldUserNames, newUserNames); err != nil {
		resp.Diagnostics.AddError("Unable to update the group", err.Error())
		return
	}

	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func updateGroup(db *DBConnection, oldName, newName string, oldUserNames, newUserNames []string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if oldName != newName {
		if newName == "" {
			return fmt.Errorf("error setting group name to an empty string")
		}
		query := fmt.Sprintf("ALTER GROUP %s RENAME TO %s", pq.QuoteIdentifier(oldName), pq.QuoteIdentifier(newName))
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating Group NAME: %w", err)
		}
	}

	if err := setGroupUsers(tx, newName, oldUserNames, newUserNames); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func setGroupUsers(tx *sql.Tx, groupName string, oldUserNames, newUserNames []string) error {
	removedUsers, addedUsers := diffStrings(oldUserNames, newUserNames)

	if len(removedUsers) > 0 {
		var removedUsersNamesSafe []string
		for _, name := range removedUsers {
			userExists, err := checkIfUserExists(tx, name)
			if err != nil {
				return err
			}

			if userExists {
				removedUsersNamesSafe = append(removedUsersNamesSafe, pq.QuoteIdentifier(name))
			}
		}

		if len(removedUsersNamesSafe) > 0 {
			query := fmt.Sprintf("ALTER GROUP %s DROP USER %s", pq.QuoteIdentifier(groupName), strings.Join(removedUsersNamesSafe, ", "))
			if _, err := tx.Exec(query); err != nil {
				return err
			}
		}
	}

	if len(addedUsers) > 0 {
		var addedUsersNamesSafe []string
		for _, name := range addedUsers {
			addedUsersNamesSafe = append(addedUsersNamesSafe, pq.QuoteIdentifier(name))
		}

		query := fmt.Sprintf("ALTER GROUP %s ADD USER %s", pq.QuoteIdentifier(groupName), strings.Join(addedUsersNamesSafe, ", "))
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func checkIfUserExists(tx *sql.Tx, name string) (bool, error) {
	var result int
	err := tx.QueryRow("SELECT 1 FROM pg_user_info WHERE usename=$1", name).Scan(&result)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading info about user: %s", err)
	}

	return true, nil
}

func (r *groupResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model groupResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	err := retryOnPQErrors(ctx, func() error { return deleteGroup(db, model.Name.ValueString()) })
	if err != nil {
		resp.Diagnostics.AddError("Unable to delete the group", err.Error())
	}
}

func deleteGroup(db *DBConnection, groupName string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	rows, err := tx.Query("SELECT nspname FROM pg_namespace WHERE nspowner != 1 OR nspname = 'public'")
	if err != nil {
		return err
	}

	var schemaNames []string

	for rows.Next() {
		var schemaName string
		if err = rows.Scan(&schemaName); err != nil {
			_ = rows.Close()
			return err
		}
		schemaNames = append(schemaNames, schemaName)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}

	for _, schemaName := range schemaNames {
		if _, err := tx.Exec(fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM GROUP %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(groupName))); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON TABLES FROM GROUP %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(groupName))); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(fmt.Sprintf("DROP GROUP %s", pq.QuoteIdentifier(groupName))); err != nil {
		return err
	}

	return tx.Commit()
}
