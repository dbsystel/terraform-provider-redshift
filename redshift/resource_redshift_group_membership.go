package redshift

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/setvalidator"
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

var (
	_ resource.Resource                = &groupMembershipResource{}
	_ resource.ResourceWithConfigure   = &groupMembershipResource{}
	_ resource.ResourceWithImportState = &groupMembershipResource{}
)

func newGroupMembershipResource() resource.Resource {
	return &groupMembershipResource{}
}

type groupMembershipResource struct {
	frameworkClient
}

type groupMembershipResourceModel struct {
	ID    types.String `tfsdk:"id"`
	Name  types.String `tfsdk:"name"`
	Users types.Set    `tfsdk:"users"`
}

func (r *groupMembershipResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_group_membership"
}

func (r *groupMembershipResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: fmt.Sprintf(`
Manages Redshift group memberships. Allows either to exclusively manage group memberships or to add members to an existing group. Note: this resource conflicts with the %s attribute of the %s resource
`, "`users`", "`redshift_group`"),
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The group membership ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			groupNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the user group.",
				Validators: []validator.String{
					stringvalidator.LengthBetween(1, 127),
				},
				// A renamed group is a different group: the old membership has to be
				// dropped and the new one added, which the SDK resource did by hand.
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			groupUsersAttr: schema.SetAttribute{
				Required:    true,
				ElementType: types.StringType,
				Description: "List of the user names to add to the group. Note: this resource does not check whether the specified users exist.",
				Validators: []validator.Set{
					setvalidator.SizeAtLeast(1),
				},
			},
		},
	}
}

func (r *groupMembershipResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *groupMembershipResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *groupMembershipResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model groupMembershipResourceModel
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

	groupName := model.Name.ValueString()
	if err := addUsersToGroup(db, groupName, userNames); err != nil {
		resp.Diagnostics.AddError("Unable to add the users to the group", err.Error())
		return
	}

	model.ID = types.StringValue(generateGroupMembershipId(groupName, userNames))
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func addUsersToGroup(db *DBConnection, group string, userNames []string) error {
	if len(userNames) == 0 {
		return nil
	}
	userNamesParam := buildUserStringArray(userNames, false)
	query := fmt.Sprintf("ALTER GROUP %s ADD USER %s;", pq.QuoteIdentifier(group), userNamesParam)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("could not add users %s to group %q: %w", userNamesParam, group, err)
	}
	return nil
}

func (r *groupMembershipResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model groupMembershipResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
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

	groupName := model.Name.ValueString()
	exists, err := groupMembershipExists(db, groupName, userNames)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the group membership", err.Error())
		return
	}
	if !exists {
		resp.State.RemoveResource(ctx)
		return
	}

	model.ID = types.StringValue(generateGroupMembershipId(groupName, userNames))
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func groupMembershipExists(db *DBConnection, groupName string, userNames []string) (bool, error) {
	userNamesParam := buildUserStringArray(userNames, true)

	query := fmt.Sprintf(
		`SELECT 1 FROM pg_group pgg JOIN pg_user pgu ON pgu.usesysid = ANY(pgg.grolist) WHERE pgg.groname = %s AND pgu.usename IN (%s);`,
		pq.QuoteLiteral(groupName), userNamesParam,
	)

	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	exists := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("could not read group membership for group %q: %w", groupName, err)
	}
	return exists, nil
}

func (r *groupMembershipResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state groupMembershipResourceModel
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

	groupName := plan.Name.ValueString()
	deletedUserNames, addedUserNames := calculateUserNamesDiff(oldUserNames, newUserNames)
	if err := dropUsersFromGroup(db, groupName, deletedUserNames); err != nil {
		resp.Diagnostics.AddError("Unable to remove users from the group", err.Error())
		return
	}
	if err := addUsersToGroup(db, groupName, addedUserNames); err != nil {
		resp.Diagnostics.AddError("Unable to add users to the group", err.Error())
		return
	}

	plan.ID = types.StringValue(generateGroupMembershipId(groupName, newUserNames))
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func calculateUserNamesDiff(oldUserNames, newUserNames []string) (deletedUserNames, addedUserNames []string) {
	deletedUserNames = make([]string, 0)
	addedUserNames = make([]string, 0)
	for _, oldUserName := range oldUserNames {
		found := false
		for _, newUserName := range newUserNames {
			if oldUserName == newUserName {
				found = true
				break
			}
		}
		if !found {
			deletedUserNames = append(deletedUserNames, oldUserName)
		}
	}
	for _, newUserName := range newUserNames {
		found := false
		for _, oldUserName := range oldUserNames {
			if newUserName == oldUserName {
				found = true
				break
			}
		}
		if !found {
			addedUserNames = append(addedUserNames, newUserName)
		}
	}
	return deletedUserNames, addedUserNames
}

func (r *groupMembershipResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model groupMembershipResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
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

	if err := dropUsersFromGroup(db, model.Name.ValueString(), userNames); err != nil {
		resp.Diagnostics.AddError("Unable to remove the users from the group", err.Error())
	}
}

func dropUsersFromGroup(db *DBConnection, groupName string, userNames []string) error {
	if len(userNames) == 0 {
		return nil
	}
	userNamesParam := buildUserStringArray(userNames, false)
	query := fmt.Sprintf("ALTER GROUP %s DROP USER %s;", pq.QuoteIdentifier(groupName), userNamesParam)

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("could not remove users %s from group %q: %w", userNamesParam, groupName, err)
	}
	return nil
}

func buildUserStringArray(userNames []string, encodeAsLiteral bool) string {
	var userNamesSafe []string
	for _, userName := range userNames {
		encodedUserName := strings.ToLower(userName)
		if encodeAsLiteral {
			encodedUserName = pq.QuoteLiteral(encodedUserName)
		} else {
			encodedUserName = pq.QuoteIdentifier(encodedUserName)
		}
		userNamesSafe = append(userNamesSafe, encodedUserName)
	}
	return strings.Join(userNamesSafe, ", ")
}

func generateGroupMembershipId(groupName string, userNames []string) string {
	var idBuilder strings.Builder
	idBuilder.WriteString(groupName)
	for _, userName := range userNames {
		idBuilder.WriteString("_")
		idBuilder.WriteString(strings.ToLower(userName))
	}
	return idBuilder.String()
}
