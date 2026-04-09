package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	assumeRoleGrantRoleNameAttr    = "iam_role"
	assumeRoleGrantGrantToTypeAttr = "grant_to_type"
	assumeRoleGrantGrantToNameAttr = "grant_to_name"
	assumeRoleGrantPrivilegesAttr  = "privileges"
)

func redshiftAssumeRoleGrant() *schema.Resource {
	return &schema.Resource{
		Description: `
Grants the permission to use an IAM role to a user or a role.

For more information, see [GRANT documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html).
`,
		CreateContext: ResourceFunc(ResourceRetryOnPQErrors(resourceRedshiftAssumeRoleGrantCreate)),
		ReadContext:   ResourceFunc(ResourceRetryOnPQErrors(resourceRedshiftAssumeRoleGrantRead)),
		DeleteContext: ResourceFunc(ResourceRetryOnPQErrors(resourceRedshiftAssumeRoleGrantDelete)),

		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			assumeRoleGrantRoleNameAttr: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringNotInSlice([]string{"default", "all"}, true),
				Description:  "The ARN of the role to be granted. 'default' and 'ALL' cannot be used in this resource.",
			},
			assumeRoleGrantGrantToTypeAttr: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				Description:  "The type of principal to grant the role to. Valid values are: 'USER', 'ROLE'.",
				ValidateFunc: validation.StringInSlice([]string{"USER", "ROLE"}, false),
			},
			assumeRoleGrantGrantToNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the user, or role to grant this role to.",
			},
			assumeRoleGrantPrivilegesAttr: {
				Type:     schema.TypeSet,
				Required: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					StateFunc: func(val interface{}) string {
						return strings.ToLower(val.(string))
					},
					ValidateFunc: validation.StringNotInSlice([]string{"all"}, true),
				},
				Set:         schema.HashString,
				Description: "The list of privileges to apply. See [GRANT command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html) to see what privileges are available. 'ALL' cannot be used in this resource.",
			},
		},
	}
}

func resourceRedshiftAssumeRoleGrantCreate(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(assumeRoleGrantRoleNameAttr).(string)
	grantToType := d.Get(assumeRoleGrantGrantToTypeAttr).(string)
	grantToName := d.Get(assumeRoleGrantGrantToNameAttr).(string)

	var privileges []string
	for _, p := range d.Get(assumeRoleGrantPrivilegesAttr).(*schema.Set).List() {
		privileges = append(privileges, p.(string))
	}

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("GRANT ASSUMEROLE ON %s", pq.QuoteLiteral(roleName))
	switch grantToType {
	case "USER":
		query = fmt.Sprintf("%s TO %s",
			query,
			grantToName,
		)
	case "ROLE":
		query = fmt.Sprintf("%s TO ROLE %s",
			query,
			grantToName,
		)
	default:
		return fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}
	query = fmt.Sprintf("%s FOR %s", query, strings.Join(privileges, ","))

	log.Printf("[DEBUG] %s\n", query)

	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("could not grant assumerole: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	d.SetId(generateAssureRoleGrantID(roleName, strings.Join(privileges, ","), grantToType, grantToName))

	return resourceRedshiftAssumeRoleGrantRead(db, d)
}

func resourceRedshiftAssumeRoleGrantRead(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(assumeRoleGrantRoleNameAttr).(string)
	grantToType := d.Get(assumeRoleGrantGrantToTypeAttr).(string)
	grantToName := d.Get(assumeRoleGrantGrantToNameAttr).(string)

	var query string
	var copy, unload, externalFunction, createModel bool

	query = `
		SELECT
			COALESCE(MAX(CASE WHEN command_type = 'COPY' THEN 1 ELSE 0 END), 0),
			COALESCE(MAX(CASE WHEN command_type = 'UNLOAD' THEN 1 ELSE 0 END), 0),
			COALESCE(MAX(CASE WHEN command_type = 'EXTERNAL FUNCTION' THEN 1 ELSE 0 END), 0),
			COALESCE(MAX(CASE WHEN command_type = 'CREATE MODEL' THEN 1 ELSE 0 END), 0)
		FROM SVV_IAM_PRIVILEGES
		WHERE iam_arn = $1
			AND identity_name = $2
			AND identity_type = LOWER($3)
		`

	log.Printf("[DEBUG] %s, $1=%s, $2=%s\n", query, roleName, grantToName)

	err := db.QueryRow(query, roleName, grantToName, grantToType).Scan(&copy, &unload, &externalFunction, &createModel)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[WARN] Assume role grant for %s to %s %s not found", roleName, grantToType, grantToName)
			d.SetId("")
			return nil
		}
		return fmt.Errorf("failed to collect privileges: %w", err)
	}

	var privileges []string
	appendIfTrue(copy, "copy", &privileges)
	appendIfTrue(unload, "unload", &privileges)
	appendIfTrue(externalFunction, "external function", &privileges)
	appendIfTrue(createModel, "create model", &privileges)

	d.Set(assumeRoleGrantPrivilegesAttr, privileges)

	return nil
}

func resourceRedshiftAssumeRoleGrantDelete(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(assumeRoleGrantRoleNameAttr).(string)
	grantToType := d.Get(assumeRoleGrantGrantToTypeAttr).(string)
	grantToName := d.Get(assumeRoleGrantGrantToNameAttr).(string)
	var privileges []string
	for _, p := range d.Get(assumeRoleGrantPrivilegesAttr).(*schema.Set).List() {
		privileges = append(privileges, p.(string))
	}

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("REVOKE ASSUMEROLE ON %s", pq.QuoteLiteral(roleName))
	switch grantToType {
	case "USER":
		query = fmt.Sprintf("%s FROM %s",
			query,
			grantToName,
		)
	case "ROLE":
		query = fmt.Sprintf("%s FROM ROLE %s",
			query,
			grantToName,
		)
	default:
		return fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}
	query = fmt.Sprintf("%s FOR %s", query, strings.Join(privileges, ","))

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

func generateAssureRoleGrantID(roleName, privilege, grantToType, grantToName string) string {
	return fmt.Sprintf("role;%s;%s;%s;%s",
		strings.ToLower(roleName),
		strings.ToLower(privilege),
		strings.ToLower(grantToType),
		strings.ToLower(grantToName))
}
