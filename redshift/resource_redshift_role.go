package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	roleNameAttr = "name"
)

func redshiftRole() *schema.Resource {
	return &schema.Resource{
		Description: `
Manages a Redshift role. Roles are named collections of privileges that can be granted to users, groups, or other roles.
Roles allow you to create a hierarchy of permissions, where a role can inherit privileges from other roles.

For more information, see [Redshift Roles Documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_roles-managing.html).
`,
		CreateContext: ResourceFunc(resourceRedshiftRoleCreate),
		ReadContext:   ResourceFunc(resourceRedshiftRoleRead),
		UpdateContext: ResourceFunc(resourceRedshiftRoleUpdate),
		DeleteContext: ResourceFunc(
			ResourceRetryOnPQErrors(resourceRedshiftRoleDelete),
		),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			roleNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the role. Role names are case-insensitive and must be unique within the database.",
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
		},
	}
}

func resourceRedshiftRoleCreate(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(roleNameAttr).(string)

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("CREATE ROLE %s", pq.QuoteIdentifier(roleName))
	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("could not create redshift role: %w", err)
	}

	// Query SVV_ROLES to get the role info (similar to how datashares use SVV_DATASHARES)
	// SVV_ROLES should have: role_name, role_owner, role_id
	var roleId string
	query = "SELECT role_name FROM SVV_ROLES WHERE role_name = $1"
	log.Printf("[DEBUG] %s, $1=%s\n", query, strings.ToLower(roleName))
	if err := tx.QueryRow(query, strings.ToLower(roleName)).Scan(&roleId); err != nil {
		return fmt.Errorf("could not verify role creation for %q: %w", roleName, err)
	}

	// Use role name as ID (similar to datashare using share_id)
	d.SetId(strings.ToLower(roleName))

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceRedshiftRoleRead(db, d)
}

func resourceRedshiftRoleRead(db *DBConnection, d *schema.ResourceData) error {
	var roleName string

	// Query SVV_ROLES (similar to SVV_DATASHARES pattern)
	query := "SELECT role_name FROM SVV_ROLES WHERE role_name = $1"
	log.Printf("[DEBUG] %s, $1=%s\n", query, d.Id())

	err := db.QueryRow(query, d.Id()).Scan(&roleName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[WARN] Redshift Role (%s) not found", d.Id())
			d.SetId("")
			return nil
		}
		return fmt.Errorf("error reading role: %w", err)
	}

	d.Set(roleNameAttr, roleName)

	return nil
}

func resourceRedshiftRoleUpdate(db *DBConnection, d *schema.ResourceData) error {
	if d.HasChange(roleNameAttr) {
		oldNameRaw, newNameRaw := d.GetChange(roleNameAttr)
		oldName := oldNameRaw.(string)
		newName := newNameRaw.(string)

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

		// Update the ID to the new name
		d.SetId(strings.ToLower(newName))
	}

	return resourceRedshiftRoleRead(db, d)
}

func resourceRedshiftRoleDelete(db *DBConnection, d *schema.ResourceData) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	// Check if role exists in SVV_ROLES
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM SVV_ROLES WHERE role_name = $1)"
	if err := tx.QueryRow(query, d.Id()).Scan(&exists); err != nil {
		return err
	}

	if !exists {
		log.Printf("[WARN] Role with name %s does not exist.\n", d.Id())
		return nil
	}

	// Drop the role
	query = fmt.Sprintf("DROP ROLE %s", pq.QuoteIdentifier(d.Id()))
	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("error dropping role: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}
