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

func redshiftDefaultPrivileges() *schema.Resource {
	return &schema.Resource{
		Description: `Defines the default set of access privileges to be applied to objects that are created in the future by the specified user. By default, users can change only their own default access privileges. Only a superuser can specify default privileges for other users.`,
		ReadContext: ResourceFunc(resourceRedshiftDefaultPrivilegesRead),
		CreateContext: ResourceFunc(
			ResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesCreate),
		),
		DeleteContext: ResourceFunc(
			ResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesDelete),
		),
		// Since we revoke all when creating, we can use create as update
		UpdateContext: ResourceFunc(
			ResourceRetryOnPQErrors(resourceRedshiftDefaultPrivilegesCreate),
		),

		Schema: map[string]*schema.Schema{
			defaultPrivilegesSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "If set, the specified default privileges are applied to new objects created in the specified schema. In this case, the user or user group that is the target of ALTER DEFAULT PRIVILEGES must have CREATE privilege for the specified schema. Default privileges that are specific to a schema are added to existing global default privileges. By default, default privileges are applied globally to the entire database.",
			},
			defaultPrivilegesGroupAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{defaultPrivilegesGroupAttr, defaultPrivilegesUserAttr, defaultPrivilegesRoleAttr},
				Description:  "The name of the  group to which the specified default privileges are applied.",
			},
			defaultPrivilegesUserAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{defaultPrivilegesGroupAttr, defaultPrivilegesUserAttr, defaultPrivilegesRoleAttr},
				Description:  "The name of the user to which the specified default privileges are applied.",
			},
			defaultPrivilegesRoleAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{defaultPrivilegesGroupAttr, defaultPrivilegesUserAttr, defaultPrivilegesRoleAttr},
				Description:  "The name of the role to which the specified default privileges are applied.",
			},
			defaultPrivilegesOwnerAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the user for which default privileges are defined. Only a superuser can specify default privileges for other users.",
			},
			defaultPrivilegesObjectTypeAttr: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice(defaultPrivilegesAllowedObjectTypes, false),
				Description:  "The Redshift object type to set the default privileges on (one of: " + strings.Join(defaultPrivilegesAllowedObjectTypes, ", ") + ").",
			},
			defaultPrivilegesPrivilegesAttr: {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					StateFunc: func(val interface{}) string {
						return strings.ToLower(val.(string))
					},
				},
				Set:         schema.HashString,
				Description: "The list of privileges to apply as default privileges. See [ALTER DEFAULT PRIVILEGES command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_DEFAULT_PRIVILEGES.html) to see what privileges are available to which object type.",
			},
		},
	}
}

func resourceRedshiftDefaultPrivilegesDelete(db *DBConnection, d *schema.ResourceData) error {
	revokeAlterDefaultQuery := createAlterDefaultsRevokeQuery(d)

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if _, err := tx.Exec(revokeAlterDefaultQuery); err != nil {
		return err
	}

	return tx.Commit()
}

func resourceRedshiftDefaultPrivilegesCreate(db *DBConnection, d *schema.ResourceData) error {
	privilegesSet := d.Get(defaultPrivilegesPrivilegesAttr).(*schema.Set)
	objectType := d.Get(defaultPrivilegesObjectTypeAttr).(string)

	var privileges []string
	for _, p := range privilegesSet.List() {
		privileges = append(privileges, strings.ToUpper(p.(string)))
	}

	if !validatePrivileges(privileges, objectType) {
		return fmt.Errorf(`invalid privileges list %+v for object type %q`, privileges, objectType)
	}

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	revokeAlterDefaultQuery := createAlterDefaultsRevokeQuery(d)
	if _, err := tx.Exec(revokeAlterDefaultQuery); err != nil {
		return err
	}

	if len(privileges) > 0 {
		alterDefaultQuery := createAlterDefaultsGrantQuery(d, privileges)
		if _, err := tx.Exec(alterDefaultQuery); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	d.SetId(generateDefaultPrivilegesID(d))

	return resourceRedshiftDefaultPrivilegesReadImpl(db, d)
}

func resourceRedshiftDefaultPrivilegesRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceRedshiftDefaultPrivilegesReadImpl(db, d)
}

func resourceRedshiftDefaultPrivilegesReadImpl(db *DBConnection, d *schema.ResourceData) error {
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	log.Printf("[DEBUG] getting ID for owner %s\n", ownerName)
	ownerID, err := getUserIDFromName(tx, ownerName)
	if err != nil {
		return fmt.Errorf("failed to get user ID: %w", err)
	}

	switch strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string)) {
	case "TABLE":
		log.Println("[DEBUG] reading default privileges")
		if err := readGroupTableDefaultPrivileges(tx, d, ownerID); err != nil {
			return fmt.Errorf("failed to read table privileges: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

func readGroupTableDefaultPrivileges(tx *sql.Tx, d *schema.ResourceData, ownerID int) error {
	var tableSelect, tableUpdate, tableInsert, tableDelete, tableDrop, tableReferences, tableRule, tableTrigger bool

	var entityName string
	var entityType string
	var query string

	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)

	if groupName, groupNameSet := d.GetOk(defaultPrivilegesGroupAttr); groupNameSet {
		entityName = groupName.(string)
		entityType = "group"
	} else if userName, userNameSet := d.GetOk(defaultPrivilegesUserAttr); userNameSet {
		entityName = userName.(string)
		entityType = "user"
	} else if roleName, roleNameSet := d.GetOk(defaultPrivilegesRoleAttr); roleNameSet {
		entityName = roleName.(string)
		entityType = "role"
	}

	query = `
		SELECT
			COALESCE(MAX(CASE WHEN privilege_type = 'SELECT' THEN 1 ELSE 0 END), 0) AS SELECT,
			COALESCE(MAX(CASE WHEN privilege_type = 'UPDATE' THEN 1 ELSE 0 END), 0) AS UPDATE,
			COALESCE(MAX(CASE WHEN privilege_type = 'INSERT' THEN 1 ELSE 0 END), 0) AS INSERT,
			COALESCE(MAX(CASE WHEN privilege_type = 'DELETE' THEN 1 ELSE 0 END), 0) AS DELETE,
			COALESCE(MAX(CASE WHEN privilege_type = 'DROP' THEN 1 ELSE 0 END), 0) AS DROP,
			COALESCE(MAX(CASE WHEN privilege_type = 'REFERENCES' THEN 1 ELSE 0 END), 0) AS REFERENCES,
			COALESCE(MAX(CASE WHEN privilege_type = 'RULE' THEN 1 ELSE 0 END), 0) AS RULE,
			COALESCE(MAX(CASE WHEN privilege_type = 'TRIGGER' THEN 1 ELSE 0 END), 0) AS TRIGGER
		FROM svv_default_privileges
		WHERE object_type = 'RELATION'
			AND grantee_name = $1
			AND grantee_type = $2
			AND owner_id = $3
			AND (($4::boolean = false AND schema_name IS NULL) OR ($4::boolean = true AND schema_name = $5))
		`

	if err := tx.QueryRow(query, entityName, entityType, ownerID, schemaNameSet, schemaName).Scan(
		&tableSelect,
		&tableUpdate,
		&tableInsert,
		&tableDelete,
		&tableDrop,
		&tableReferences,
		&tableRule,
		&tableTrigger); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to collect privileges: %w", err)
	}

	var privileges []string
	appendIfTrue(tableSelect, "select", &privileges)
	appendIfTrue(tableUpdate, "update", &privileges)
	appendIfTrue(tableInsert, "insert", &privileges)
	appendIfTrue(tableDelete, "delete", &privileges)
	appendIfTrue(tableDrop, "drop", &privileges)
	appendIfTrue(tableReferences, "references", &privileges)
	appendIfTrue(tableRule, "rule", &privileges)
	appendIfTrue(tableTrigger, "trigger", &privileges)

	log.Printf("[DEBUG] Collected privileges for entity %s %s: %v\n", entityType, entityName, privileges)

	d.Set(defaultPrivilegesPrivilegesAttr, privileges)

	return nil
}

func generateDefaultPrivilegesID(d *schema.ResourceData) string {
	var entityName, schemaName string

	if groupName, isGroup := d.GetOk(defaultPrivilegesGroupAttr); isGroup {
		entityName = fmt.Sprintf("gn:%s", groupName.(string))
	} else if userName, isUser := d.GetOk(defaultPrivilegesUserAttr); isUser {
		entityName = fmt.Sprintf("un:%s", userName.(string))
	} else if roleName, isRole := d.GetOk(defaultPrivilegesRoleAttr); isRole {
		entityName = fmt.Sprintf("rn:%s", roleName.(string))
	}

	if schemaNameRaw, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr); schemaNameSet {
		schemaName = fmt.Sprintf("sn:%s", schemaNameRaw.(string))
	} else {
		schemaName = "noschema"
	}

	ownerName := fmt.Sprintf("on:%s", d.Get(defaultPrivilegesOwnerAttr).(string))
	objectType := fmt.Sprintf("ot:%s", d.Get(defaultPrivilegesObjectTypeAttr).(string))

	return strings.Join([]string{
		entityName, schemaName, ownerName, objectType,
	}, "_")
}

func createAlterDefaultsGrantQuery(d *schema.ResourceData, privileges []string) string {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)
	objectType := strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string))

	var entityName, toWhomIndicator string
	if groupName, isGroup := d.GetOk(defaultPrivilegesGroupAttr); isGroup {
		entityName = groupName.(string)
		toWhomIndicator = "GROUP"
	} else if userName, isUser := d.GetOk(defaultPrivilegesUserAttr); isUser {
		entityName = userName.(string)
	} else if roleName, isRole := d.GetOk(defaultPrivilegesRoleAttr); isRole {
		entityName = roleName.(string)
		toWhomIndicator = "ROLE"
	}

	alterQuery := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s", pq.QuoteIdentifier(ownerName))

	if schemaNameSet {
		alterQuery = fmt.Sprintf("%s IN SCHEMA %s", alterQuery, pq.QuoteIdentifier(schemaName.(string)))
	}

	return fmt.Sprintf(
		"%s GRANT %s ON %sS TO %s %s",
		alterQuery,
		strings.Join(privileges, ","),
		objectType,
		toWhomIndicator,
		pq.QuoteIdentifier(entityName),
	)
}

func createAlterDefaultsRevokeQuery(d *schema.ResourceData) string {
	schemaName, schemaNameSet := d.GetOk(defaultPrivilegesSchemaAttr)
	ownerName := d.Get(defaultPrivilegesOwnerAttr).(string)
	objectType := strings.ToUpper(d.Get(defaultPrivilegesObjectTypeAttr).(string))

	var entityName, fromWhomIndicator string
	if groupName, isGroup := d.GetOk(defaultPrivilegesGroupAttr); isGroup {
		entityName = groupName.(string)
		fromWhomIndicator = "GROUP"
	} else if userName, isUser := d.GetOk(defaultPrivilegesUserAttr); isUser {
		entityName = userName.(string)
	} else if roleName, isRole := d.GetOk(defaultPrivilegesRoleAttr); isRole {
		entityName = roleName.(string)
		fromWhomIndicator = "ROLE"
	}

	alterQuery := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s", pq.QuoteIdentifier(ownerName))

	if schemaNameSet {
		alterQuery = fmt.Sprintf("%s IN SCHEMA %s", alterQuery, pq.QuoteIdentifier(schemaName.(string)))
	}

	return fmt.Sprintf(
		"%s REVOKE ALL PRIVILEGES ON %sS FROM %s %s",
		alterQuery,
		objectType,
		fromWhomIndicator,
		pq.QuoteIdentifier(entityName),
	)
}
