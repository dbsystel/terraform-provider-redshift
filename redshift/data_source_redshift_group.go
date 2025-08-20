package redshift

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func dataSourceRedshiftGroup() *schema.Resource {
	return &schema.Resource{
		Description: `
Groups are collections of users who are all granted whatever privileges are associated with the group. You can use groups to assign privileges by role. For example, you can create different groups for sales, administration, and support and give the users in each group the appropriate access to the data they require for their work. You can grant or revoke privileges at the group level, and those changes will apply to all members of the group, except for superusers.
		`,
		ReadContext: ResourceFunc(dataSourceRedshiftGroupRead),
		Schema: map[string]*schema.Schema{
			groupNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "Name of the user group. Group names beginning with two underscores are reserved for Amazon Redshift internal use.",
				ValidateFunc: validation.StringDoesNotMatch(regexp.MustCompile("^__.*"), "Group names beginning with two underscores are reserved for Amazon Redshift internal use"),
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			groupUsersAttr: {
				Type:     schema.TypeSet,
				Computed: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "List of the user names who belong to the group",
			},
		},
	}
}

func dataSourceRedshiftGroupRead(db *DBConnection, d *schema.ResourceData) error {
	var (
		groupId    string
		groupUsers []string
	)

	groupName := d.Get(groupNameAttr).(string)

	query := `SELECT u.usename, g.grosysid FROM pg_user_info u, pg_group g WHERE g.groname = $1 AND u.usesysid = ANY(g.grolist);`
	rows, err := db.Query(query, groupName)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return fmt.Errorf("could not read group members for group name %q: %w", groupName, err)
		}
		var userName string
		if err := rows.Scan(&userName, &groupId); err != nil {
			return fmt.Errorf("could not read group members for group name %q: %w", groupName, err)
		}
		groupUsers = append(groupUsers, userName)
	}
	if len(groupUsers) == 0 {
		// no users found so the group id could not be fetched, we have to query for the name
		query = `SELECT grosysid FROM pg_group WHERE groname = $1;`
		if err := db.QueryRow(query, groupName).Scan(&groupId); err != nil {
			return err
		}
	}

	d.SetId(groupId)
	d.Set(groupUsersAttr, groupUsers)
	return nil
}
