package redshift

import (
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

func redshiftGroupMembership() *schema.Resource {
	return &schema.Resource{
		Description: fmt.Sprintf(`
Manages Redshift group memberships. Allows either to exclusively manage group memberships or to add members to an existing group. Note: this resource conflicts with the %s attribute of the %s resource
`, "`users`", "`redshift_group`"),
		CreateContext: ResourceFunc(resourceRedshiftGroupMembershipCreate),
		ReadContext:   ResourceFunc(resourceRedshiftGroupMembershipRead),
		UpdateContext: ResourceFunc(resourceRedshiftGroupMembershipUpdate),
		DeleteContext: ResourceFunc(resourceRedshiftGroupMembershipDelete),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			groupNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				Description:  "Name of the user group.",
				ValidateFunc: validation.StringLenBetween(1, 127),
			},
			groupUsersAttr: {
				Type:        schema.TypeSet,
				Required:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "List of the user names to add to the group. Note: this resource does not check whether the specified users exist.",
			},
		},
	}
}

func resourceRedshiftGroupMembershipCreate(db *DBConnection, d *schema.ResourceData) error {
	groupName := d.Get(groupNameAttr).(string)
	userNames := parseUserNames(d.Get(groupUsersAttr))

	if len(userNames) == 0 {
		return fmt.Errorf("at least one user must be specified in %q", groupUsersAttr)
	}

	if err := addUsersToGroup(db, groupName, userNames); err != nil {
		return err
	}

	return resourceRedshiftGroupMembershipRead(db, d)
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

func resourceRedshiftGroupMembershipRead(db *DBConnection, d *schema.ResourceData) error {
	groupName := d.Get(groupNameAttr).(string)
	userNames := parseUserNames(d.Get(groupUsersAttr))

	userNamesParam := buildUserStringArray(userNames, true)

	query := fmt.Sprintf(
		`SELECT 1 FROM pg_group pgg JOIN pg_user pgu ON pgu.usesysid = ANY(pgg.grolist) WHERE pgg.groname = %s AND pgu.usename IN (%s);`,
		pq.QuoteLiteral(groupName), userNamesParam,
	)

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		if err = rows.Err(); err != nil {
			return fmt.Errorf("could not read group membership for group %q: %w", groupName, err)
		}
		d.SetId(generateGroupMembershipId(groupName, userNames))
	} else {
		d.SetId("")
	}
	return nil
}

func resourceRedshiftGroupMembershipUpdate(db *DBConnection, d *schema.ResourceData) error {
	rawUserNamesOld, rawUserNamesNew := d.GetChange(groupUsersAttr)
	oldUserNames := parseUserNames(rawUserNamesOld)
	newUserNames := parseUserNames(rawUserNamesNew)
	if len(newUserNames) == 0 {
		return fmt.Errorf("at least one user must be specified in %q", groupUsersAttr)
	}
	if d.HasChange(groupNameAttr) {
		if err := resourceRedshiftGroupMembershipDelete(db, d); err != nil {
			return fmt.Errorf("error deleting group membership while updating the resource: %w", err)
		}
		if err := resourceRedshiftGroupMembershipCreate(db, d); err != nil {
			return fmt.Errorf("error creating group membership while updating the resource: %w", err)
		}
		return nil
	}
	deletedUserNames, addedUserNames := calculateUserNamesDiff(oldUserNames, newUserNames)
	if err := dropUsersFromGroup(db, d.Get(groupNameAttr).(string), deletedUserNames); err != nil {
		return fmt.Errorf("error removing users from group while updating the resource: %w", err)
	}
	if err := addUsersToGroup(db, d.Get(groupNameAttr).(string), addedUserNames); err != nil {
		return fmt.Errorf("error adding users to group while updating the resource: %w", err)
	}
	return resourceRedshiftGroupMembershipRead(db, d)
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

func resourceRedshiftGroupMembershipDelete(db *DBConnection, d *schema.ResourceData) error {
	groupName := d.Get(groupNameAttr).(string)
	userNames := parseUserNames(d.Get(groupUsersAttr))

	return dropUsersFromGroup(db, groupName, userNames)
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

func parseUserNames(rawUserNames interface{}) []string {
	rawUserNamesTyped := rawUserNames.(*schema.Set).List()
	userNames := make([]string, len(rawUserNamesTyped))
	for index, userNameRaw := range rawUserNamesTyped {
		userNames[index] = userNameRaw.(string)
	}
	return userNames
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
