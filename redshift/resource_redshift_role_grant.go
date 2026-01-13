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
	roleGrantRoleNameAttr    = "role_name"
	roleGrantGrantToTypeAttr = "grant_to_type"
	roleGrantGrantToNameAttr = "grant_to_name"
)

func redshiftRoleGrant() *schema.Resource {
	return &schema.Resource{
		Description: `
Grants a role to a user or another role. This allows hierarchical role-based access control in Redshift.

When a role is granted to another role, the recipient role inherits all privileges of the granted role. 
This enables role inheritance chains where permissions can be organized hierarchically.

For more information, see [GRANT documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html).
`,
		CreateContext: ResourceFunc(resourceRedshiftRoleGrantCreate),
		ReadContext:   ResourceFunc(resourceRedshiftRoleGrantRead),
		DeleteContext: ResourceFunc(resourceRedshiftRoleGrantDelete),

		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			roleGrantRoleNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role to grant.",
				StateFunc: func(val any) string {
					return strings.ToLower(val.(string))
				},
			},
			roleGrantGrantToTypeAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The type of principal to grant the role to. Valid values are: 'user' or 'role'.",
				ValidateFunc: func(val any, key string) (warns []string, errs []error) {
					v := strings.ToLower(val.(string))
					if v != "user" && v != "role" {
						errs = append(errs, fmt.Errorf("%q must be one of: 'user', 'role', got: %s", key, val))
					}
					return
				},
				StateFunc: func(val any) string {
					return strings.ToLower(val.(string))
				},
			},
			roleGrantGrantToNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the user, or role to grant this role to.",
				StateFunc: func(val any) string {
					return strings.ToLower(val.(string))
				},
			},
		},
	}
}

func resourceRedshiftRoleGrantCreate(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(roleGrantRoleNameAttr).(string)
	grantToType := strings.ToUpper(d.Get(roleGrantGrantToTypeAttr).(string))
	grantToName := d.Get(roleGrantGrantToNameAttr).(string)

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

	// Generate ID: role:rolename:type:name
	d.SetId(generateRoleGrantID(roleName, grantToType, grantToName))

	return resourceRedshiftRoleGrantRead(db, d)
}

func resourceRedshiftRoleGrantRead(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(roleGrantRoleNameAttr).(string)
	grantToType := d.Get(roleGrantGrantToTypeAttr).(string) // Already lowercase from StateFunc
	grantToName := d.Get(roleGrantGrantToNameAttr).(string)

	var exists int
	var query string

	switch strings.ToUpper(grantToType) {
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
		return fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}

	log.Printf("[DEBUG] %s, $1=%s, $2=%s\n", query, roleName, grantToName)

	err := db.QueryRow(query, roleName, grantToName).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[WARN] Role grant %s to %s %s not found", roleName, grantToType, grantToName)
			d.SetId("")
			return nil
		}
		return fmt.Errorf("error reading role grant: %w", err)
	}

	return nil
}

func resourceRedshiftRoleGrantDelete(db *DBConnection, d *schema.ResourceData) error {
	// Parse ID to get the values to revoke
	// ID format: "role:rolename:type:targetname"
	parts := strings.Split(d.Id(), ":")
	if len(parts) != 4 {
		return fmt.Errorf("invalid role grant ID format: %s", d.Id())
	}

	roleName := parts[1]
	grantToType := strings.ToUpper(parts[2])
	grantToName := parts[3]

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
