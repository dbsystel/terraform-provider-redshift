package redshift

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = &groupDataSource{}
	_ datasource.DataSourceWithConfigure = &groupDataSource{}
)

func newGroupDataSource() datasource.DataSource {
	return &groupDataSource{}
}

type groupDataSource struct {
	frameworkClient
}

type groupDataSourceModel struct {
	ID    types.String `tfsdk:"id"`
	Name  types.String `tfsdk:"name"`
	Users types.Set    `tfsdk:"users"`
}

func (d *groupDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_group"
}

func (d *groupDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Groups are collections of users who are all granted whatever privileges are associated with the group. You can use groups to assign privileges by role. For example, you can create different groups for sales, administration, and support and give the users in each group the appropriate access to the data they require for their work. You can grant or revoke privileges at the group level, and those changes will apply to all members of the group, except for superusers.
		`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The group ID.",
			},
			groupNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the user group. Group names beginning with two underscores are reserved for Amazon Redshift internal use.",
				Validators: []validator.String{
					regexDoesNotMatch(regexp.MustCompile("^__.*"), "Group names beginning with two underscores are reserved for Amazon Redshift internal use"),
				},
			},
			groupUsersAttr: schema.SetAttribute{
				Computed:    true,
				ElementType: types.StringType,
				Description: "List of the user names who belong to the group",
			},
		},
	}
}

func (d *groupDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.configureDataSource(req, resp)
}

func (d *groupDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var model groupDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := d.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	// pg_group stores the lower-cased group name.
	groupName := strings.ToLower(model.Name.ValueString())
	groupID, groupUsers, err := readGroupMembers(db, groupName)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the group", err.Error())
		return
	}

	users, diags := types.SetValueFrom(ctx, types.StringType, groupUsers)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	model.ID = types.StringValue(groupID)
	model.Users = users

	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func readGroupMembers(db *DBConnection, groupName string) (string, []string, error) {
	var (
		groupID    string
		groupUsers []string
	)

	query := `SELECT u.usename, g.grosysid FROM pg_user_info u, pg_group g WHERE g.groname = $1 AND u.usesysid = ANY(g.grolist);`
	rows, err := db.Query(query, groupName)
	if err != nil {
		return "", nil, err
	}
	for rows.Next() {
		var userName string
		if err = rows.Scan(&userName, &groupID); err != nil {
			_ = rows.Close()
			return "", nil, fmt.Errorf("could not read group members for group name %q: %w", groupName, err)
		}
		groupUsers = append(groupUsers, userName)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return "", nil, fmt.Errorf("could not read group members for group name %q: %w", groupName, err)
	}
	if err = rows.Close(); err != nil {
		return "", nil, fmt.Errorf("could not close group members rows for group name %q: %w", groupName, err)
	}

	if len(groupUsers) == 0 {
		// no users found so the group id could not be fetched, we have to query for the name
		query = `SELECT grosysid FROM pg_group WHERE groname = $1;`
		if err := db.QueryRow(query, groupName).Scan(&groupID); err != nil {
			return "", nil, err
		}
	}

	return groupID, groupUsers, nil
}
