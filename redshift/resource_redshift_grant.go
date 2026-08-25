package redshift

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"slices"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	grantUserAttr       = "user"
	grantGroupAttr      = "group"
	grantRoleAttr       = "role"
	grantDatabaseAttr   = "database"
	grantSchemaAttr     = "schema"
	grantObjectTypeAttr = "object_type"
	grantObjectsAttr    = "objects"
	grantPrivilegesAttr = "privileges"

	grantToPublicName = "public"
)

var grantAllowedObjectTypes = []string{
	"table",
	"schema",
	"database",
	"function",
	"procedure",
	"language",
}

var grantObjectTypesCodes = map[string][]string{
	"table":     {"r", "m", "v"},
	"procedure": {"p"},
	"function":  {"f"},
}

var (
	_ resource.Resource              = &grantResource{}
	_ resource.ResourceWithConfigure = &grantResource{}
)

func newGrantResource() resource.Resource {
	return &grantResource{}
}

type grantResource struct {
	frameworkClient
}

type grantResourceModel struct {
	ID         types.String `tfsdk:"id"`
	User       types.String `tfsdk:"user"`
	Group      types.String `tfsdk:"group"`
	Role       types.String `tfsdk:"role"`
	SchemaName types.String `tfsdk:"schema"`
	Database   types.String `tfsdk:"database"`
	ObjectType types.String `tfsdk:"object_type"`
	Objects    types.Set    `tfsdk:"objects"`
	Privileges types.Set    `tfsdk:"privileges"`

	// objects and privileges as plain strings, filled once per operation.
	objects    []string
	privileges []string
}

func (r *grantResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_grant"
}

func (r *grantResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Defines access privileges for users and  groups. Privileges include access options such as being able to read data in tables and views, write data, create tables, and drop tables. Use this command to give specific privileges for a table, database, schema, function, procedure, language, or column.
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The grant ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			grantUserAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The name of the user to grant privileges on. Exactly one of `user`, `group`, or `role` must be set.",
				Validators: []validator.String{
					regexDoesNotMatch(regexp.MustCompile("^(?i)public$"), "User name cannot be 'public'. To use GRANT ... TO PUBLIC set the group name to 'public' instead."),
					stringvalidator.ExactlyOneOf(
						path.MatchRoot(grantGroupAttr),
						path.MatchRoot(grantRoleAttr),
					),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			grantGroupAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The name of the group to grant privileges on. Exactly one of `user`, `group`, or `role` must be set. Settings the group name to `public` or `PUBLIC` (it is case insensitive in this case) will result in a `GRANT ... TO PUBLIC` statement.",
				PlanModifiers: []planmodifier.String{
					// Only the special PUBLIC group is case insensitive.
					normalizeString(func(name string) string {
						if strings.ToLower(name) == grantToPublicName {
							return strings.ToLower(name)
						}
						return name
					}),
					stringplanmodifier.RequiresReplace(),
				},
			},
			grantRoleAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The name of the role to grant privileges on. Exactly one of `user`, `group`, or `role` must be set. Keep in mind: When granting to a role, the privileges are not read back from the system tables. The GRANT is executed successfully, so we trust the state.", // todo: change when role grants are read back from the system tables
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
					stringplanmodifier.RequiresReplace(),
				},
			},
			grantSchemaAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The database schema to grant privileges on.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			grantDatabaseAttr: schema.StringAttribute{
				Optional:    true,
				Description: "The name of the database to grant privileges on. Only used when `object_type` is `database`. By default, the database to which the provider is connected will be used",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			grantObjectTypeAttr: schema.StringAttribute{
				Required:    true,
				Description: "The Redshift object type to grant privileges on (one of: " + strings.Join(grantAllowedObjectTypes, ", ") + ").",
				Validators: []validator.String{
					stringvalidator.OneOf(grantAllowedObjectTypes...),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			grantObjectsAttr: schema.SetAttribute{
				Optional:    true,
				ElementType: types.StringType,
				Description: "The objects upon which to grant the privileges. An empty list (the default) means to grant permissions on all objects of the specified type; see the resource notes on grants on all objects in a schema for what to expect. Ignored when `object_type` is one of (`database`, `schema`).",
				PlanModifiers: []planmodifier.Set{
					normalizeSet(strings.ToLower),
					setplanmodifier.RequiresReplace(),
				},
			},
			grantPrivilegesAttr: schema.SetAttribute{
				Required:    true,
				ElementType: types.StringType,
				Description: "The list of privileges to apply as default privileges. See [GRANT command documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html) to see what privileges are available to which object type. An empty list could be provided to revoke all privileges for this user or group. Required when `object_type` is set to `language`.",
				PlanModifiers: []planmodifier.Set{
					// "temporary" is spelled "temp" everywhere Redshift reports it back.
					normalizeSet(func(privilege string) string {
						privilege = strings.ToLower(privilege)
						if privilege == "temporary" {
							return "temp"
						}
						return privilege
					}),
				},
			},
		},
	}
}

func (r *grantResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *grantResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model grantResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	r.apply(ctx, model, &resp.Diagnostics, &resp.State)
}

func (r *grantResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// Applying revokes everything first, so it doubles as the update.
	var model grantResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	r.apply(ctx, model, &resp.Diagnostics, &resp.State)
}

func (r *grantResource) apply(ctx context.Context, model grantResourceModel, diagnostics *diag.Diagnostics, state *tfsdk.State) {
	db := r.connect(diagnostics)
	if db == nil {
		return
	}

	model.objects = stringsFromSet(ctx, model.Objects, diagnostics)
	model.privileges = stringsFromSet(ctx, model.Privileges, diagnostics)
	if diagnostics.HasError() {
		return
	}

	if err := validateGrant(model); err != nil {
		diagnostics.AddError("Invalid grant", err.Error())
		return
	}

	err := retryOnPQErrors(ctx, func() error { return applyGrants(db, model) })
	if err != nil {
		diagnostics.AddError("Unable to apply the grants", err.Error())
		return
	}

	model.ID = types.StringValue(generateGrantID(model))
	diagnostics.Append(state.Set(ctx, model)...)
}

func validateGrant(model grantResourceModel) error {
	objectType := model.ObjectType.ValueString()
	schemaName := model.SchemaName.ValueString()

	if (objectType == "table" || objectType == "function" || objectType == "procedure") && schemaName == "" {
		return fmt.Errorf("parameter `%s` is required for objects of type table, function and procedure", grantSchemaAttr)
	}

	if (objectType == "database" || objectType == "schema") && len(model.objects) > 0 {
		return fmt.Errorf("cannot specify `%s` when `%s` is `database` or `schema`", grantObjectsAttr, grantObjectTypeAttr)
	}

	if objectType == "language" && len(model.objects) == 0 {
		return fmt.Errorf("parameter `%s` is required for objects of type language", grantObjectsAttr)
	}

	if !validatePrivileges(model.privileges, objectType) {
		return fmt.Errorf(`invalid privileges list %+v for object of type %q`, model.privileges, objectType)
	}

	return nil
}

func applyGrants(db *DBConnection, model grantResourceModel) error {
	databaseName := getDatabaseName(db, model)

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	if err := revokeGrants(tx, databaseName, model); err != nil {
		return err
	}

	if err := createGrants(tx, databaseName, model); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (r *grantResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model grantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	model.objects = stringsFromSet(ctx, model.Objects, &resp.Diagnostics)
	model.privileges = stringsFromSet(ctx, model.Privileges, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	err := retryOnPQErrors(ctx, func() error {
		databaseName := getDatabaseName(db, model)

		tx, err := startTransaction(db.client)
		if err != nil {
			return err
		}
		defer deferredRollback(tx)

		if err := revokeGrants(tx, databaseName, model); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to revoke the grants", err.Error())
	}
}

func (r *grantResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model grantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	model.objects = stringsFromSet(ctx, model.Objects, &resp.Diagnostics)
	model.privileges = stringsFromSet(ctx, model.Privileges, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	privileges, err := readGrants(db, model)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the grants", err.Error())
		return
	}

	// A nil result means there was nothing to read back, in which case the privileges
	// already in state are kept rather than reported as drift.
	if privileges != nil {
		granted, diags := types.SetValueFrom(ctx, types.StringType, privileges)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		model.Privileges = granted
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func readGrants(db *DBConnection, model grantResourceModel) ([]string, error) {
	objectType := model.ObjectType.ValueString()

	switch objectType {
	case "database":
		return readDatabaseGrants(db, model)
	case "schema":
		return readSchemaGrants(db, model)
	case "table":
		return readTableGrants(db, model)
	case "function", "procedure":
		return readCallableGrants(db, model)
	case "language":
		return readLanguageGrants(db, model)
	default:
		return nil, fmt.Errorf("unsupported %s: %q", grantObjectTypeAttr, objectType)
	}
}

func readDatabaseGrants(db *DBConnection, model grantResourceModel) ([]string, error) {
	databaseName := getDatabaseName(db, model)

	query := `
SELECT sdp.privilege_type
FROM svv_database_privileges sdp
WHERE sdp.database_name = $1
AND sdp.identity_type = $2
AND sdp.identity_name = $3;`

	return readIdentityPrivileges(db, model, "database", databaseName, query)
}

func readSchemaGrants(db *DBConnection, model grantResourceModel) ([]string, error) {
	schemaName := model.SchemaName.ValueString()

	query := `
SELECT 
    ssp.privilege_type
FROM svv_schema_privileges ssp
WHERE ssp.namespace_name = $1
AND identity_type = $2
AND identity_name = $3`

	return readIdentityPrivileges(db, model, "schema", schemaName, query)
}

func readTableGrants(db *DBConnection, model grantResourceModel) ([]string, error) {
	log.Printf("[DEBUG] Reading table grants")

	var entityName, query string
	var queryArgs []interface{}
	isUser := !model.User.IsNull()
	isGroup := !model.Group.IsNull()
	isRole := !model.Role.IsNull()
	databaseName := getDatabaseName(db, model)
	schemaName := model.SchemaName.ValueString()
	objects := model.objects

	// The pg_class-based queries below exclude the internal storage tables that
	// back materialized views (named "mv_tbl__<view>__<n>"). GRANT ... ON ALL
	// TABLES does not touch those tables, so including them would leave the
	// intersection permanently missing the granted privilege. The role query
	// reads from svv_all_tables, which does not surface them.
	if isUser {
		entityName = model.User.ValueString()
		query = `
  SELECT
    relname,
    decode(charindex('r',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS SELECT,
    decode(charindex('w',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS UPDATE,
    decode(charindex('a',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS INSERT,
    decode(charindex('d',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS DELETE,
    decode(charindex('D',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS DROP,
    decode(charindex('x',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS REFERENCES,
    decode(charindex('P',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS TRUNCATE,
    decode(charindex('A',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'group '||u.usename), u.usename||'=', 2) ,'/',1)),NULL,0,0,0,1) AS ALTER
  FROM pg_user u, pg_class cl
  JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
  WHERE
    cl.relkind = ANY($1)
    AND cl.relname NOT LIKE 'mv\_tbl\_\_%'
    AND u.usename=$2
    AND nsp.nspname=$3
`
		queryArgs = []interface{}{
			pq.Array(grantObjectTypesCodes["table"]), entityName, schemaName,
		}
	} else if isGroup {
		entityName = model.Group.ValueString()
		query = `
  SELECT
    relname,
    decode(charindex('r',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS SELECT,
    decode(charindex('w',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS UPDATE,
    decode(charindex('a',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS INSERT,
    decode(charindex('d',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS DELETE,
    decode(charindex('D',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS DROP,
    decode(charindex('x',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS REFERENCES,
    decode(charindex('P',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS TRUNCATE,
    decode(charindex('A',split_part(split_part(replace(array_to_string(relacl, '|'), '"', ''),'group ' || gr.groname || '=',2 ) ,'/',1)), NULL,0, 0,0, 1) AS ALTER
  FROM pg_group gr, pg_class cl
  JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
  WHERE
    cl.relkind = ANY($1)
    AND cl.relname NOT LIKE 'mv\_tbl\_\_%'
    AND gr.groname=$2
    AND nsp.nspname=$3
`
		queryArgs = []interface{}{
			pq.Array(grantObjectTypesCodes["table"]), entityName, schemaName,
		}
	} else if isRole {
		entityName = model.Role.ValueString()
		query = `
  SELECT
    t.table_name,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'SELECT' THEN 1 ELSE 0 END), 0) AS SELECT,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'UPDATE' THEN 1 ELSE 0 END), 0) AS UPDATE,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'INSERT' THEN 1 ELSE 0 END), 0) AS INSERT,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'DELETE' THEN 1 ELSE 0 END), 0) AS DELETE,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'DROP' THEN 1 ELSE 0 END), 0) AS DROP,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'REFERENCES' THEN 1 ELSE 0 END), 0) AS REFERENCES,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'TRUNCATE' THEN 1 ELSE 0 END), 0) AS TRUNCATE,
    COALESCE(MAX(CASE WHEN p.privilege_type = 'ALTER' THEN 1 ELSE 0 END), 0) AS ALTER
  FROM SVV_ALL_TABLES t
  LEFT JOIN svv_relation_privileges p
    ON p.relation_name = t.table_name
    AND p.namespace_name = t.schema_name
    AND p.identity_name = $1
    AND p.identity_type = 'role'
  WHERE t.schema_name = $2
    and t.database_name = $3
  GROUP BY t.table_name
`
		queryArgs = []interface{}{
			entityName, schemaName, databaseName,
		}
	}

	if isGrantToPublic(model) {
		query = `
		SELECT
		  relname,
		  decode(charindex('r',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS SELECT,
		  decode(charindex('w',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS UPDATE,
		  decode(charindex('a',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS INSERT,
		  decode(charindex('d',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS DELETE,
		  decode(charindex('D',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS DROP,
		  decode(charindex('x',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS REFERENCES,
		  decode(charindex('P',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS TRUNCATE,
		  decode(charindex('A',split_part(split_part(regexp_replace(replace(array_to_string(relacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)),NULL,0,0,0,1) AS ALTER
		FROM pg_class cl
		JOIN pg_namespace nsp ON nsp.oid = cl.relnamespace
		WHERE
		  cl.relkind = ANY($1)
		  AND cl.relname NOT LIKE 'mv\_tbl\_\_%'
		  AND nsp.nspname=$2
	  `
		queryArgs = []interface{}{
			pq.Array(grantObjectTypesCodes["table"]), schemaName,
		}
	}

	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// The query returns one row per in-scope table (all tables in the schema
	// when objects is empty, otherwise every relation matching relkind). We
	// aggregate by intersection: a privilege is reported present only if EVERY
	// relevant table grants it to the grantee. This reflects the invariant an
	// "ALL TABLES IN SCHEMA" grant maintains and is independent of row order.
	var privileges []string
	seenTable := false
	for rows.Next() {
		var objName string
		var tableSelect, tableUpdate, tableInsert, tableDelete, tableDrop, tableReferences, tableTruncate, tableAlter bool

		if err := rows.Scan(&objName, &tableSelect, &tableUpdate, &tableInsert, &tableDelete, &tableDrop, &tableReferences, &tableTruncate, &tableAlter); err != nil {
			return nil, err
		}

		if len(objects) > 0 && !slices.Contains(objects, objName) {
			continue
		}

		var tablePrivileges []string
		appendIfTrue(tableSelect, "select", &tablePrivileges)
		appendIfTrue(tableUpdate, "update", &tablePrivileges)
		appendIfTrue(tableInsert, "insert", &tablePrivileges)
		appendIfTrue(tableDelete, "delete", &tablePrivileges)
		appendIfTrue(tableDrop, "drop", &tablePrivileges)
		appendIfTrue(tableReferences, "references", &tablePrivileges)
		appendIfTrue(tableTruncate, "truncate", &tablePrivileges)
		appendIfTrue(tableAlter, "alter", &tablePrivileges)

		if !seenTable {
			privileges = tablePrivileges
			seenTable = true
		} else {
			privileges = intersectStrings(privileges, tablePrivileges)
		}

		log.Printf("[DEBUG] Collected table grants; table: '%v'; privileges: %v; for: %s", objName, tablePrivileges, entityName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// No in-scope tables were found (empty schema, or none of the named objects
	// exist). There is nothing to read back, so leave the configured privileges
	// in state. Reporting an empty set here would be permanent drift that no
	// apply could resolve, since there are no tables to grant on.
	if !seenTable {
		return nil, nil
	}

	return privileges, nil
}

func readCallableGrants(db *DBConnection, model grantResourceModel) ([]string, error) {
	log.Printf("[DEBUG] Reading callable grants")

	var entityName, query string
	var queryArgs []interface{}

	isUser := !model.User.IsNull()
	isGroup := !model.Group.IsNull()
	isRole := !model.Role.IsNull()
	schemaName := model.SchemaName.ValueString()
	objectType := model.ObjectType.ValueString()

	databaseName := getDatabaseName(db, model)

	if isUser {
		entityName = model.User.ValueString()
		query = `
	SELECT
		proname,
		decode(nvl(charindex('X',split_part(split_part(regexp_replace(replace(array_to_string(pr.proacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0), 0,0,1) AS EXECUTE
	FROM pg_proc_info pr
		JOIN pg_namespace nsp ON nsp.oid = pr.pronamespace,
	pg_user u
	WHERE
		nsp.nspname=$1 
		AND u.usename=$2
		AND pr.prokind=ANY($3)
`
		queryArgs = []interface{}{
			schemaName, entityName, pq.Array(grantObjectTypesCodes[objectType]),
		}
	} else if isGroup {
		entityName = model.Group.ValueString()
		query = `
	SELECT
		proname,
		decode(nvl(charindex('X',split_part(split_part(replace(array_to_string(pr.proacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0), 0,0,1) AS EXECUTE
	FROM pg_proc_info pr
		JOIN pg_namespace nsp ON nsp.oid = pr.pronamespace,
	pg_group gr
	WHERE
		nsp.nspname=$1 
    AND gr.groname=$2
		AND pr.prokind=ANY($3)
`
		queryArgs = []interface{}{
			schemaName, entityName, pq.Array(grantObjectTypesCodes[objectType]),
		}
	} else if isRole {
		entityName = model.Role.ValueString()
		query = `
	SELECT
		p.function_name,
		COALESCE(MAX(CASE WHEN p.privilege_type = 'EXECUTE' THEN 1 ELSE 0 END), 0) AS EXECUTE
	FROM svv_function_privileges p
	JOIN svv_redshift_functions pr ON pr.function_name = p.function_name AND pr.schema_name = p.namespace_name
	WHERE p.namespace_name = $1
		AND p.identity_name = $2
		AND p.identity_type = 'role'
        AND pr.database_name = $3
	GROUP BY p.function_name
`
		queryArgs = []interface{}{
			schemaName, entityName, databaseName,
		}
	}

	callables := stripArgumentsFromCallablesDefinitions(model.objects)

	if isGrantToPublic(model) {
		query = `
	SELECT
		proname,
		decode(nvl(charindex('X',split_part(split_part(regexp_replace(replace(array_to_string(pr.proacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0), 0,0,1) AS EXECUTE
	FROM pg_proc_info pr
		JOIN pg_namespace nsp ON nsp.oid = pr.pronamespace
	WHERE
		nsp.nspname=$1 
		AND pr.prokind=ANY($2)
`
		queryArgs = []interface{}{
			schemaName, pq.Array(grantObjectTypesCodes[objectType]),
		}
	}

	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	privileges := []string{}
	for rows.Next() {
		var objName string
		var callableExecute bool

		if err := rows.Scan(&objName, &callableExecute); err != nil {
			return nil, err
		}
		if len(callables) > 0 && !slices.Contains(callables, objName) {
			continue
		}

		if callableExecute && !slices.Contains(privileges, "execute") {
			privileges = append(privileges, "execute")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("[DEBUG] Reading callable grants - Done")

	return privileges, nil
}

func readLanguageGrants(db *DBConnection, model grantResourceModel) ([]string, error) {
	log.Printf("[DEBUG] Reading language grants")

	var entityName, query string

	isUser := !model.User.IsNull()
	isGroup := !model.Group.IsNull()
	isRole := !model.Role.IsNull()

	if isUser {
		entityName = model.User.ValueString()
		query = `
  SELECT
		lanname,
    decode(nvl(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(lg.lanacl, '|'), '"', ''),'group '||u.usename,'__avoidGroupPrivs__'), u.usename||'=', 2) ,'/',1)), 0), 0,0,1) AS USAGE
  FROM pg_language lg, pg_user u
  WHERE
    u.usename=$1
`
	} else if isGroup {
		entityName = model.Group.ValueString()
		query = `
  SELECT
		lanname,
    decode(nvl(charindex('U',split_part(split_part(replace(array_to_string(lg.lanacl, '|'), '"', ''),'group ' || gr.groname,2 ) ,'/',1)), 0), 0,0,1) AS USAGE
  FROM pg_language lg, pg_group gr
  WHERE
    gr.groname=$1
`
	} else if isRole {
		entityName = model.Role.ValueString()
		query = `
SELECT
	p.language_name,
	COALESCE(MAX(CASE WHEN p.privilege_type = 'USAGE' THEN 1 ELSE 0 END), 0) AS USAGE
FROM svv_language_privileges p
JOIN pg_language lg ON lg.lanname = p.language_name
WHERE p.identity_name = $1
	AND p.identity_type = 'role'
GROUP BY p.language_name
`
	}

	queryArgs := []interface{}{entityName}

	// Handle GRANT TO PUBLIC
	if isGrantToPublic(model) {
		query = `
		SELECT
			  lanname,
		  decode(nvl(charindex('U',split_part(split_part(regexp_replace(replace(array_to_string(lg.lanacl, '|'), '"', ''),'[^|]+=','__avoidUserPrivs__'), '=', 2) ,'/',1)), 0), 0,0,1) AS USAGE
		FROM pg_language lg
	  `
		queryArgs = []interface{}{}
	}

	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objects := model.objects

	// Intersection across all in-scope languages, matching readTableGrants:
	// report a privilege only if every relevant language grants it.
	var privileges []string
	seenLanguage := false
	for rows.Next() {
		var objName string
		var languageUsage bool

		if err := rows.Scan(&objName, &languageUsage); err != nil {
			return nil, err
		}

		if len(objects) > 0 && !slices.Contains(objects, objName) {
			continue
		}

		var languagePrivileges []string
		appendIfTrue(languageUsage, "usage", &languagePrivileges)

		if !seenLanguage {
			privileges = languagePrivileges
			seenLanguage = true
		} else {
			privileges = intersectStrings(privileges, languagePrivileges)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// No in-scope languages were found: nothing to read back, so leave the
	// configured privileges in state rather than reporting permanent drift.
	if !seenLanguage {
		log.Printf("[DEBUG] Reading language grants - Done")
		return nil, nil
	}

	log.Printf("[DEBUG] Reading language grants - Done")

	return privileges, nil
}

func readIdentityPrivileges(db *DBConnection, model grantResourceModel, objectType, objectName, query string) ([]string, error) {
	identityType, identityName := getGrantIdentity(model)

	rows, err := db.Query(query, objectName, identityType, identityName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	privileges := []string{}
	for rows.Next() {
		var privilege string
		if err := rows.Scan(&privilege); err != nil {
			return nil, err
		}
		privileges = append(privileges, strings.ToLower(privilege))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("[DEBUG] Collected %s %q privileges for %s %q: %v", objectType, objectName, identityType, identityName, privileges)

	return privileges, nil
}

func getGrantIdentity(model grantResourceModel) (string, string) {
	if isGrantToPublic(model) {
		return "public", "public"
	}

	switch {
	case !model.User.IsNull():
		return "user", model.User.ValueString()
	case !model.Group.IsNull():
		return "group", model.Group.ValueString()
	case !model.Role.IsNull():
		return "role", model.Role.ValueString()
	}

	return "", ""
}

func revokeGrants(tx *sql.Tx, databaseName string, model grantResourceModel) error {
	query := createGrantsRevokeQuery(model, databaseName)
	_, err := tx.Exec(query)
	return err
}

func createGrants(tx *sql.Tx, databaseName string, model grantResourceModel) error {
	if len(model.privileges) == 0 {
		log.Printf("[DEBUG] no privileges to grant for %s", model.Group.ValueString())
		return nil
	}

	query := createGrantsQuery(model, databaseName)
	_, err := tx.Exec(query)
	return err
}

func createGrantsRevokeQuery(model grantResourceModel, databaseName string) string {
	var query, toWhomIndicator, entityName string

	switch {
	case !model.Group.IsNull():
		toWhomIndicator = "GROUP"
		entityName = model.Group.ValueString()
	case !model.User.IsNull():
		entityName = model.User.ValueString()
	case !model.Role.IsNull():
		toWhomIndicator = "ROLE"
		entityName = model.Role.ValueString()
	}

	fromEntityName := pq.QuoteIdentifier(entityName)
	if isGrantToPublic(model) {
		toWhomIndicator = ""
		fromEntityName = "PUBLIC"
	}

	switch strings.ToUpper(model.ObjectType.ValueString()) {
	case "DATABASE":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s %s",
			pq.QuoteIdentifier(databaseName),
			toWhomIndicator,
			fromEntityName,
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s %s",
			pq.QuoteIdentifier(model.SchemaName.ValueString()),
			toWhomIndicator,
			fromEntityName,
		)
	case "TABLE":
		objects := model.objects
		if len(objects) > 0 {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON %s %s FROM %s %s",
				strings.ToUpper(model.ObjectType.ValueString()),
				setToPgIdentList(objects, model.SchemaName.ValueString()),
				toWhomIndicator,
				fromEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM %s %s",
				strings.ToUpper(model.ObjectType.ValueString()),
				pq.QuoteIdentifier(model.SchemaName.ValueString()),
				toWhomIndicator,
				fromEntityName,
			)
		}
	case "FUNCTION", "PROCEDURE":
		objects := model.objects
		if len(objects) > 0 {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON %s %s FROM %s %s",
				strings.ToUpper(model.ObjectType.ValueString()),
				setToPgIdentListNotQuoted(objects, model.SchemaName.ValueString()),
				toWhomIndicator,
				fromEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM %s %s",
				strings.ToUpper(model.ObjectType.ValueString()),
				pq.QuoteIdentifier(model.SchemaName.ValueString()),
				toWhomIndicator,
				fromEntityName,
			)
		}
	case "LANGUAGE":
		objects := model.objects
		query = fmt.Sprintf(
			"REVOKE USAGE ON LANGUAGE %s FROM %s %s",
			setToPgIdentList(objects, ""),
			toWhomIndicator,
			fromEntityName,
		)
	}
	log.Printf("[DEBUG] Created REVOKE query: %s", query)
	return query
}

func createGrantsQuery(model grantResourceModel, databaseName string) string {
	var query, toWhomIndicator, entityName string
	privileges := model.privileges

	switch {
	case !model.Group.IsNull():
		toWhomIndicator = "GROUP"
		entityName = model.Group.ValueString()
	case !model.User.IsNull():
		entityName = model.User.ValueString()
	case !model.Role.IsNull():
		toWhomIndicator = "ROLE"
		entityName = model.Role.ValueString()
	}

	toEntityName := pq.QuoteIdentifier(entityName)
	if isGrantToPublic(model) {
		toWhomIndicator = ""
		toEntityName = "PUBLIC"
	}

	switch strings.ToUpper(model.ObjectType.ValueString()) {
	case "DATABASE":
		query = fmt.Sprintf(
			"GRANT %s ON DATABASE %s TO %s %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(databaseName),
			toWhomIndicator,
			toEntityName,
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"GRANT %s ON SCHEMA %s TO %s %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(model.SchemaName.ValueString()),
			toWhomIndicator,
			toEntityName,
		)
	case "TABLE", "LANGUAGE":
		objects := model.objects
		if len(objects) > 0 {
			query = fmt.Sprintf(
				"GRANT %s ON %s %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(model.ObjectType.ValueString()),
				setToPgIdentList(objects, model.SchemaName.ValueString()),
				toWhomIndicator,
				toEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(model.ObjectType.ValueString()),
				pq.QuoteIdentifier(model.SchemaName.ValueString()),
				toWhomIndicator,
				toEntityName,
			)
		}
	case "FUNCTION", "PROCEDURE":
		objects := model.objects
		if len(objects) > 0 {
			query = fmt.Sprintf(
				"GRANT %s ON %s %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(model.ObjectType.ValueString()),
				setToPgIdentListNotQuoted(objects, model.SchemaName.ValueString()),
				toWhomIndicator,
				toEntityName,
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO %s %s",
				strings.Join(privileges, ","),
				strings.ToUpper(model.ObjectType.ValueString()),
				pq.QuoteIdentifier(model.SchemaName.ValueString()),
				toWhomIndicator,
				toEntityName,
			)
		}
	}

	log.Printf("[DEBUG] Created GRANT query: %s", query)
	return query
}

func getDatabaseName(db *DBConnection, model grantResourceModel) string {
	if !model.Database.IsNull() && model.Database.ValueString() != "" {
		return model.Database.ValueString()
	}
	return db.client.config.Database
}

func isGrantToPublic(model grantResourceModel) bool {
	return !model.Group.IsNull() && strings.ToLower(model.Group.ValueString()) == grantToPublicName
}

func generateGrantID(model grantResourceModel) string {
	var parts []string

	if !model.Group.IsNull() {
		name := model.Group.ValueString()
		if isGrantToPublic(model) {
			name = strings.ToLower(name)
		}
		parts = append(parts, fmt.Sprintf("gn:%s", name))
	}

	if !model.User.IsNull() {
		parts = append(parts, fmt.Sprintf("un:%s", model.User.ValueString()))
	}

	if !model.Role.IsNull() {
		parts = append(parts, fmt.Sprintf("rn:%s", model.Role.ValueString()))
	}

	objectType := fmt.Sprintf("ot:%s", model.ObjectType.ValueString())
	parts = append(parts, objectType)

	if objectType != "ot:database" && objectType != "ot:language" {
		parts = append(parts, model.SchemaName.ValueString())
	}

	parts = append(parts, model.objects...)

	return strings.Join(parts, "_")
}
