package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	groupNameAttr  = "name"
	groupUsersAttr = "users"
)

func redshiftGroup() *schema.Resource {
	return &schema.Resource{
		Description: `
Groups are collections of users who are all granted whatever privileges are associated with the group. You can use groups to assign privileges by role. For example, you can create different groups for sales, administration, and support and give the users in each group the appropriate access to the data they require for their work. You can grant or revoke privileges at the group level, and those changes will apply to all members of the group, except for superusers.
`,
		CreateContext: ResourceFunc(resourceRedshiftGroupCreate),
		ReadContext:   ResourceFunc(resourceRedshiftGroupRead),
		UpdateContext: ResourceFunc(resourceRedshiftGroupUpdate),
		DeleteContext: ResourceFunc(
			ResourceRetryOnPQErrors(resourceRedshiftGroupDelete),
		),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

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
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "List of the user names to add to the group",
			},
		},
	}
}

func resourceRedshiftGroupRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftGroupReadImpl(db, d)
}

func resourceRedshiftGroupReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var (
		groupName  string
		groupUsers []string
	)

	query := `SELECT groname, u.usename FROM pg_user_info u, pg_group g WHERE g.grosysid = $1 AND u.usesysid = ANY(g.grolist);`
	rows, err := db.Query(query, d.Id())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return fmt.Errorf("could not read group members for group id %q: %w", d.Id(), err)
		}
		var userName string
		if err := rows.Scan(&groupName, &userName); err != nil {
			return fmt.Errorf("could not read group members for group id %q: %w", d.Id(), err)
		}
		groupUsers = append(groupUsers, userName)
	}
	if len(groupUsers) == 0 {
		// no users found so the group name could not be fetched, we have to query for the name
		query = `SELECT groname FROM pg_group WHERE grosysid = $1;`
		if err := db.QueryRow(query, d.Id()).Scan(&groupName); err != nil {
			return err
		}
	}

	d.Set(groupNameAttr, groupName)
	d.Set(groupUsersAttr, groupUsers)

	return nil
}

func resourceRedshiftGroupCreate(db *DBConnection, d *schema.ResourceData) error {
	groupName := d.Get(groupNameAttr).(string)

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("CREATE GROUP %s", pq.QuoteIdentifier(groupName))
	if v, ok := d.GetOk(groupUsersAttr); ok && len(v.(*schema.Set).List()) > 0 {
		usernames := v.(*schema.Set).List()

		var usernamesSafe []string
		for _, name := range usernames {
			usernamesSafe = append(usernamesSafe, pq.QuoteIdentifier(name.(string)))
		}

		query = fmt.Sprintf("%s WITH USER %s", query, strings.Join(usernamesSafe, ", "))
	}

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("could not create redshift group: %w", err)
	}

	var groSysID string
	if err := tx.QueryRow("SELECT grosysid FROM pg_group WHERE groname = $1", strings.ToLower(groupName)).Scan(&groSysID); err != nil {
		return fmt.Errorf("could not get redshift group id for %q: %w", groupName, err)
	}

	d.SetId(groSysID)

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftGroupReadImpl(db, d)
}

func resourceRedshiftGroupDelete(db *DBConnection, d *schema.ResourceData) error {
	groupName := d.Get(groupNameAttr).(string)

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	rows, err := tx.Query("SELECT nspname FROM pg_namespace WHERE nspowner != 1 OR nspname = 'public'")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return err
		}

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

func resourceRedshiftGroupUpdate(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := setGroupName(tx, d); err != nil {
		return err
	}

	if err := setUsersNames(tx, db, d); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftGroupReadImpl(db, d)
}

func setGroupName(tx *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(groupNameAttr) {
		return nil
	}

	oldRaw, newRaw := d.GetChange(groupNameAttr)
	oldValue := oldRaw.(string)
	newValue := newRaw.(string)

	if newValue == "" {
		return fmt.Errorf("error setting group name to an empty string")
	}

	query := fmt.Sprintf("ALTER GROUP %s RENAME TO %s", pq.QuoteIdentifier(oldValue), pq.QuoteIdentifier(newValue))
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("error updating Group NAME: %w", err)
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

func setUsersNames(tx *sql.Tx, _ *DBConnection, d *schema.ResourceData) error {
	if !d.HasChange(groupUsersAttr) {
		return nil
	}

	groupName := d.Get(groupNameAttr).(string)
	oldUsersSet, newUsersSet := d.GetChange(groupUsersAttr)
	removedUsers := oldUsersSet.(*schema.Set).Difference(newUsersSet.(*schema.Set))
	addedUsers := newUsersSet.(*schema.Set).Difference(oldUsersSet.(*schema.Set))

	if removedUsers.Len() > 0 {
		var removedUsersNamesSafe []string
		for _, name := range removedUsers.List() {
			userExists, err := checkIfUserExists(tx, name.(string))
			if err != nil {
				return err
			}

			if userExists {
				removedUsersNamesSafe = append(removedUsersNamesSafe, pq.QuoteIdentifier(name.(string)))
			}
		}

		if len(removedUsersNamesSafe) > 0 {
			query := fmt.Sprintf("ALTER GROUP %s DROP USER %s", pq.QuoteIdentifier(groupName), strings.Join(removedUsersNamesSafe, ", "))

			if _, err := tx.Exec(query); err != nil {
				return err
			}
		}
	}

	if addedUsers.Len() > 0 {
		var addedUsersNamesSafe []string
		for _, name := range addedUsers.List() {
			addedUsersNamesSafe = append(addedUsersNamesSafe, pq.QuoteIdentifier(name.(string)))
		}

		query := fmt.Sprintf("ALTER GROUP %s ADD USER %s", pq.QuoteIdentifier(groupName), strings.Join(addedUsersNamesSafe, ", "))

		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}

	return nil
}
