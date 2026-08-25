package redshift

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	userNameAttr              = "name"
	userPasswordAttr          = "password"
	userValidUntilAttr        = "valid_until"
	userCreateDBAttr          = "create_database"
	userConnLimitAttr         = "connection_limit"
	userSyslogAccessAttr      = "syslog_access"
	userSuperuserAttr         = "superuser"
	userSessionTimeoutAttr    = "session_timeout"
	userSessionParametersAttr = "session_parameters"

	// defaults
	defaultUserSyslogAccess          = "RESTRICTED"
	defaultUserSuperuserSyslogAccess = "UNRESTRICTED"
)

// Session configuration parameter names are configuration identifiers passed to
// ALTER USER ... SET, not SQL identifiers, so they can't be quoted with
// pq.QuoteIdentifier. Restrict them to a conservative character set instead.
// Lowercase only: Redshift folds parameter names to lowercase in
// pg_user.useconfig, so an uppercase name in config would never match what's
// read back, causing a perpetual diff.
// See https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_ConfigurationRef.html
var sessionParameterNameRegexp = regexp.MustCompile(`^[a-z0-9_]+$`)

func validateSessionParameterName(name string) error {
	if !sessionParameterNameRegexp.MatchString(name) {
		return fmt.Errorf("invalid session parameter name %q: only lowercase letters, digits and underscores are allowed", name)
	}
	return nil
}

// When authenticating using temporary credentials obtained by GetClusterCredentials,
// the resulting username is prefixed with either "IAM:"" or "IAMA:"
// This regexp is designed to match either prefix.
// See https://docs.aws.amazon.com/redshift/latest/APIReference/API_GetClusterCredentials.html
var temporaryCredentialsUsernamePrefixRegexp = regexp.MustCompile("^(?:IAMA?:)")

// Resolve the "real" username by stripping the temporary credentials prefix
func permanentUsername(username string) string {
	return temporaryCredentialsUsernamePrefixRegexp.ReplaceAllString(username, "")
}

var (
	_ resource.Resource                   = &userResource{}
	_ resource.ResourceWithConfigure      = &userResource{}
	_ resource.ResourceWithImportState    = &userResource{}
	_ resource.ResourceWithValidateConfig = &userResource{}
)

func newUserResource() resource.Resource {
	return &userResource{}
}

type userResource struct {
	frameworkClient
}

type userResourceModel struct {
	ID                types.String `tfsdk:"id"`
	Name              types.String `tfsdk:"name"`
	Password          types.String `tfsdk:"password"`
	ValidUntil        types.String `tfsdk:"valid_until"`
	CreateDatabase    types.Bool   `tfsdk:"create_database"`
	ConnectionLimit   types.Int64  `tfsdk:"connection_limit"`
	SyslogAccess      types.String `tfsdk:"syslog_access"`
	Superuser         types.Bool   `tfsdk:"superuser"`
	SessionTimeout    types.Int64  `tfsdk:"session_timeout"`
	SessionParameters types.Map    `tfsdk:"session_parameters"`
}

func (r *userResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

func (r *userResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Amazon Redshift user accounts can only be created and dropped by a database superuser. Users are authenticated when they login to Amazon Redshift. They can own databases and database objects (for example, tables) and can grant privileges on those objects to users, groups, and schemas to control who has access to which object. Users with CREATE DATABASE rights can create databases and grant privileges to those databases. Superusers have database ownership privileges for all databases.
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The user ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			userNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "The name of the user account to create. The user name can't be `PUBLIC`.",
				Validators: []validator.String{
					stringvalidator.NoneOfCaseInsensitive("public"),
				},
			},
			userPasswordAttr: schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "Sets the user's password. Users can change their own passwords, unless the password is disabled. To disable password, omit this parameter or set it to `null`. Can also be a hashed password rather than the plaintext password. Please refer to the Redshift [CREATE USER documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html) for information on creating a password hash.",
			},
			userValidUntilAttr: schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Default:     stringdefault.StaticString("infinity"),
				Description: "Sets a date and time after which the user's password is no longer valid. By default the password has no time limit.",
			},
			userCreateDBAttr: schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: "Allows the user to create new databases. By default user can't create new databases.",
			},
			userConnLimitAttr: schema.Int64Attribute{
				Optional:    true,
				Computed:    true,
				Default:     int64default.StaticInt64(-1),
				Description: "The maximum number of database connections the user is permitted to have open concurrently. The limit isn't enforced for superusers.",
				Validators: []validator.Int64{
					int64validator.AtLeast(-1),
				},
			},
			userSyslogAccessAttr: schema.StringAttribute{
				Optional: true,
				// Computed, so that leaving the argument out keeps whichever default the
				// cluster applied instead of planning a change. That is what the SDK
				// resource used a DiffSuppressFunc for.
				Computed:    true,
				Description: "A clause that specifies the level of access that the user has to the Amazon Redshift system tables and views. If `RESTRICTED` (default) is specified, the user can see only the rows generated by that user in user-visible system tables and views. If `UNRESTRICTED` is specified, the user can see all rows in user-visible system tables and views, including rows generated by another user. `UNRESTRICTED` doesn't give a regular user access to superuser-visible tables. Only superusers can see superuser-visible tables.",
				Validators: []validator.String{
					stringvalidator.OneOf("RESTRICTED", "UNRESTRICTED"),
				},
			},
			userSuperuserAttr: schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: `Determine whether the user is a superuser with all database privileges.`,
			},
			userSessionTimeoutAttr: schema.Int64Attribute{
				Optional:    true,
				Computed:    true,
				Default:     int64default.StaticInt64(0),
				Description: "The maximum time in seconds that a session remains inactive or idle. The range is 60 seconds (one minute) to 1,728,000 seconds (20 days). If no session timeout is set for the user, the cluster setting applies.",
				Validators: []validator.Int64{
					int64validator.Between(60, 1728000),
				},
			},
			userSessionParametersAttr: schema.MapAttribute{
				Optional:    true,
				ElementType: types.StringType,
				Description: "A map of session configuration parameters to apply as per-user defaults via `ALTER USER ... SET <param> = <value>` (for example `query_group` or `search_path`). Removing a key issues `ALTER USER ... RESET <param>`. The provider is the exclusive owner of this map: any session parameter set on the user outside of this resource (manually or by another tool) is adopted into state and reset on the next apply. Parameter names may only contain lowercase letters, digits and underscores. See the [Amazon Redshift configuration reference](https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_ConfigurationRef.html) for the list of valid parameters.",
			},
		},
	}
}

func (r *userResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *userResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// ValidateConfig enforces the two rules the SDK resource expressed as a CustomizeDiff.
func (r *userResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	var model userResourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if model.Superuser.IsUnknown() || !model.Superuser.ValueBool() {
		return
	}

	if !model.Password.IsUnknown() && (model.Password.IsNull() || model.Password.ValueString() == "") {
		resp.Diagnostics.AddAttributeError(path.Root(userPasswordAttr), "Invalid user", "users that are superusers must define a password")
	}

	if !model.SyslogAccess.IsUnknown() && !model.SyslogAccess.IsNull() && model.SyslogAccess.ValueString() != defaultUserSuperuserSyslogAccess {
		resp.Diagnostics.AddAttributeError(path.Root(userSyslogAccessAttr), "Invalid user", fmt.Sprintf("superusers must have syslog access set to %q", defaultUserSuperuserSyslogAccess))
	}
}

func (r *userResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model userResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	// syslog_access is computed when it is not configured: the cluster gets the default
	// that matches the user kind, and state has to say which one that was.
	if model.SyslogAccess.IsUnknown() {
		model.SyslogAccess = types.StringValue(defaultUserSyslogAccess)
		if model.Superuser.ValueBool() {
			model.SyslogAccess = types.StringValue(defaultUserSuperuserSyslogAccess)
		}
	}

	sessionParameters := stringMapFromMap(ctx, model.SessionParameters, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	usesysid, err := createUser(db, model, sessionParameters)
	if err != nil {
		resp.Diagnostics.AddError("Unable to create the user", err.Error())
		return
	}

	model.ID = types.StringValue(usesysid)
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func createUser(db *DBConnection, model userResourceModel, sessionParameters map[string]string) (string, error) {
	tx, err := startTransaction(db.client)
	if err != nil {
		return "", err
	}
	defer deferredRollback(tx)

	createOpts := make([]string, 0, 8)

	if model.Password.IsNull() || model.Password.ValueString() == "" {
		createOpts = append(createOpts, "PASSWORD DISABLE")
	} else {
		createOpts = append(createOpts, fmt.Sprintf("PASSWORD '%s'", pqQuoteLiteral(model.Password.ValueString())))
	}

	validUntil := model.ValidUntil.ValueString()
	if validUntil == "" || strings.ToLower(validUntil) == "infinity" {
		validUntil = "infinity"
	}
	createOpts = append(createOpts, fmt.Sprintf("VALID UNTIL '%s'", pqQuoteLiteral(validUntil)))

	createOpts = append(createOpts, fmt.Sprintf("SYSLOG ACCESS %s", model.SyslogAccess.ValueString()))
	createOpts = append(createOpts, fmt.Sprintf("CONNECTION LIMIT %d", model.ConnectionLimit.ValueInt64()))
	if sessionTimeout := model.SessionTimeout.ValueInt64(); sessionTimeout != 0 {
		createOpts = append(createOpts, fmt.Sprintf("SESSION TIMEOUT %d", sessionTimeout))
	}

	superuserToken := "NOCREATEUSER"
	if model.Superuser.ValueBool() {
		superuserToken = "CREATEUSER"
	}
	createDatabaseToken := "NOCREATEDB"
	if model.CreateDatabase.ValueBool() {
		createDatabaseToken = "CREATEDB"
	}
	createOpts = append(createOpts, superuserToken, createDatabaseToken)

	userName := model.Name.ValueString()
	query := fmt.Sprintf("CREATE USER %s WITH %s", pq.QuoteIdentifier(userName), strings.Join(createOpts, " "))

	if _, err := tx.Exec(query); err != nil {
		return "", fmt.Errorf("error creating user %s: %w", userName, err)
	}

	var usesysid string
	if err := tx.QueryRow("SELECT usesysid FROM pg_user_info WHERE usename = $1", userName).Scan(&usesysid); err != nil {
		return "", fmt.Errorf("user does not exist in pg_user_info table: %w", err)
	}

	if err := setUserSessionParameters(tx, userName, nil, sessionParameters); err != nil {
		return "", err
	}

	if err = tx.Commit(); err != nil {
		return "", fmt.Errorf("could not commit transaction: %w", err)
	}

	return usesysid, nil
}

func (r *userResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model userResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	found, err := readUser(ctx, db, &model, &resp.Diagnostics)
	if err != nil {
		resp.Diagnostics.AddError("Unable to read the user", err.Error())
		return
	}
	if !found {
		resp.State.RemoveResource(ctx)
		return
	}
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// readUser fills the model from the cluster. It reports whether the user still exists.
// The password is never read back: the cluster does not hand it out.
func readUser(ctx context.Context, db *DBConnection, model *userResourceModel, diagnostics *diag.Diagnostics) (bool, error) {
	var userName, userValidUntil, userConnLimit, userSyslogAccess, userSessionTimeout string
	var userSuperuser, userCreateDB bool

	columns := []string{
		"user_name",
		"createdb",
		"superuser",
		"syslog_access",
		`COALESCE(connection_limit::TEXT, 'UNLIMITED')`,
		"session_timeout",
	}

	values := []interface{}{
		&userName,
		&userCreateDB,
		&userSuperuser,
		&userSyslogAccess,
		&userConnLimit,
		&userSessionTimeout,
	}

	useSysID := model.ID.ValueString()

	userSQL := fmt.Sprintf("SELECT %s FROM svv_user_info WHERE user_id = $1", strings.Join(columns, ","))
	err := db.QueryRow(userSQL, useSysID).Scan(values...)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		log.Printf("[WARN] Redshift User (%s) not found", useSysID)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading User: %w", err)
	}

	err = db.QueryRow("SELECT COALESCE(valuntil, 'infinity') FROM pg_user_info WHERE usesysid = $1", useSysID).Scan(&userValidUntil)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		log.Printf("[WARN] Redshift User (%s) not found", useSysID)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading User: %w", err)
	}

	var userConfig []string
	err = db.QueryRow("SELECT useconfig FROM pg_user WHERE usesysid = $1", useSysID).Scan(pq.Array(&userConfig))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		log.Printf("[WARN] Redshift User (%s) not found", useSysID)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading user session parameters: %w", err)
	}

	userValidUntil, err = validateAndAdjustValidUntil(userValidUntil)
	if err != nil {
		return false, err
	}

	userConnLimitNumber := -1
	if userConnLimit != "UNLIMITED" {
		if userConnLimitNumber, err = strconv.Atoi(userConnLimit); err != nil {
			return false, err
		}
	}

	userSessionTimeoutNumber, err := strconv.Atoi(userSessionTimeout)
	if err != nil {
		return false, err
	}

	sessionParameters, diags := types.MapValueFrom(ctx, types.StringType, parseUserSessionParameters(userConfig))
	diagnostics.Append(diags...)
	if diagnostics.HasError() {
		return true, nil
	}

	model.Name = types.StringValue(userName)
	model.CreateDatabase = types.BoolValue(userCreateDB)
	model.Superuser = types.BoolValue(userSuperuser)
	model.SyslogAccess = types.StringValue(userSyslogAccess)
	model.ConnectionLimit = types.Int64Value(int64(userConnLimitNumber))
	model.ValidUntil = types.StringValue(userValidUntil)
	model.SessionTimeout = types.Int64Value(int64(userSessionTimeoutNumber))
	model.SessionParameters = sessionParameters

	return true, nil
}

func (r *userResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state userResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if plan.SyslogAccess.IsUnknown() {
		plan.SyslogAccess = types.StringValue(defaultUserSyslogAccess)
		if plan.Superuser.ValueBool() {
			plan.SyslogAccess = types.StringValue(defaultUserSuperuserSyslogAccess)
		}
	}

	oldSessionParameters := stringMapFromMap(ctx, state.SessionParameters, &resp.Diagnostics)
	newSessionParameters := stringMapFromMap(ctx, plan.SessionParameters, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := updateUser(db, plan, state, oldSessionParameters, newSessionParameters); err != nil {
		resp.Diagnostics.AddError("Unable to update the user", err.Error())
		return
	}

	plan.ID = state.ID
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func updateUser(db *DBConnection, plan, state userResourceModel, oldSessionParameters, newSessionParameters map[string]string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	userName := plan.Name.ValueString()
	nameChanged := userName != state.Name.ValueString()

	if nameChanged {
		if userName == "" {
			return fmt.Errorf("error setting user name to an empty string")
		}
		query := fmt.Sprintf("ALTER USER %s RENAME TO %s", pq.QuoteIdentifier(state.Name.ValueString()), pq.QuoteIdentifier(userName))
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating User NAME: %w", err)
		}
	}

	// Renaming a user resets its password, so it is always set again afterwards.
	if nameChanged || plan.Password.ValueString() != state.Password.ValueString() {
		passwdTok := "PASSWORD DISABLE"
		if password := plan.Password.ValueString(); password != "" {
			passwdTok = fmt.Sprintf("PASSWORD '%s'", pqQuoteLiteral(password))
		}
		if _, err := tx.Exec(fmt.Sprintf("ALTER USER %s %s", pq.QuoteIdentifier(userName), passwdTok)); err != nil {
			return fmt.Errorf("error updating user password: %w", err)
		}
	}

	if plan.ConnectionLimit.ValueInt64() != state.ConnectionLimit.ValueInt64() {
		query := fmt.Sprintf("ALTER USER %s CONNECTION LIMIT %d", pq.QuoteIdentifier(userName), plan.ConnectionLimit.ValueInt64())
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating user CONNECTION LIMIT: %w", err)
		}
	}

	if plan.CreateDatabase.ValueBool() != state.CreateDatabase.ValueBool() {
		tok := "NOCREATEDB"
		if plan.CreateDatabase.ValueBool() {
			tok = "CREATEDB"
		}
		if _, err := tx.Exec(fmt.Sprintf("ALTER USER %s WITH %s", pq.QuoteIdentifier(userName), tok)); err != nil {
			return fmt.Errorf("error updating user CREATEDB: %w", err)
		}
	}

	if plan.Superuser.ValueBool() != state.Superuser.ValueBool() {
		tok := "NOCREATEUSER"
		if plan.Superuser.ValueBool() {
			tok = "CREATEUSER"
		}
		if _, err := tx.Exec(fmt.Sprintf("ALTER USER %s WITH %s", pq.QuoteIdentifier(userName), tok)); err != nil {
			return fmt.Errorf("error updating user SUPERUSER: %w", err)
		}
	}

	if validUntil := plan.ValidUntil.ValueString(); validUntil != "" && validUntil != state.ValidUntil.ValueString() {
		if strings.ToLower(validUntil) == "infinity" {
			validUntil = "infinity"
		}
		query := fmt.Sprintf("ALTER USER %s VALID UNTIL '%s'", pq.QuoteIdentifier(userName), pqQuoteLiteral(validUntil))
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating user VALID UNTIL: %w", err)
		}
	}

	if plan.SyslogAccess.ValueString() != state.SyslogAccess.ValueString() {
		query := fmt.Sprintf("ALTER USER %s WITH SYSLOG ACCESS %s", pq.QuoteIdentifier(userName), plan.SyslogAccess.ValueString())
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating user SYSLOG ACCESS: %w", err)
		}
	}

	if plan.SessionTimeout.ValueInt64() != state.SessionTimeout.ValueInt64() {
		var query string
		if plan.SessionTimeout.ValueInt64() == 0 {
			query = fmt.Sprintf("ALTER USER %s RESET SESSION TIMEOUT", pq.QuoteIdentifier(userName))
		} else {
			query = fmt.Sprintf("ALTER USER %s SESSION TIMEOUT %d", pq.QuoteIdentifier(userName), plan.SessionTimeout.ValueInt64())
		}
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating user SESSION TIMEOUT: %w", err)
		}
	}

	if err := setUserSessionParameters(tx, userName, oldSessionParameters, newSessionParameters); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func setUserSessionParameters(tx *sql.Tx, userName string, oldParams, newParams map[string]string) error {
	// RESET parameters that were removed from the map.
	for name := range oldParams {
		if _, ok := newParams[name]; ok {
			continue
		}
		if err := validateSessionParameterName(name); err != nil {
			return err
		}
		query := fmt.Sprintf("ALTER USER %s RESET %s", pq.QuoteIdentifier(userName), name)
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error resetting user session parameter %q: %w", name, err)
		}
	}

	// SET parameters that were added or changed.
	for name, value := range newParams {
		if old, ok := oldParams[name]; ok && old == value {
			continue
		}
		if err := validateSessionParameterName(name); err != nil {
			return err
		}
		query := fmt.Sprintf("ALTER USER %s SET %s = '%s'", pq.QuoteIdentifier(userName), name, pqQuoteLiteral(value))
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error setting user session parameter %q: %w", name, err)
		}
	}

	return nil
}

func (r *userResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model userResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	err := retryOnPQErrors(ctx, func() error {
		return deleteUser(db, model.ID.ValueString(), model.Name.ValueString())
	})
	if err != nil {
		resp.Diagnostics.AddError("Unable to delete the user", err.Error())
	}
}

func deleteUser(db *DBConnection, useSysID, userName string) error {
	rawUsername, err := db.client.config.GetUsername(db)
	if err != nil {
		return fmt.Errorf("error retrieving username: %w", err)
	}
	newOwnerName := permanentUsername(rawUsername)

	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	// Based on https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_find_dropuser_objs.sql
	var reassignOwnerGenerator = `SELECT owner.ddl
			FROM (
			      -- Functions owned by the user
			      SELECT pgu.usesysid,
			      'alter function ' || QUOTE_IDENT(nc.nspname) || '.' ||textin (regprocedureout (pproc.oid::regprocedure)) || ' owner to ' || $2
			      FROM pg_proc pproc,pg_user pgu,pg_namespace nc
			      WHERE pproc.pronamespace = nc.oid
			      AND   pproc.proowner = pgu.usesysid
			  UNION ALL
			      -- Databases owned by the user
			      SELECT pgu.usesysid,
			      'alter database ' || QUOTE_IDENT(pgd.datname) || ' owner to ' || $2
			      FROM pg_database pgd,
				   pg_user pgu
			      WHERE pgd.datdba = pgu.usesysid
			  UNION ALL
			      -- Schemas owned by the user
			      SELECT pgu.usesysid,
			      'alter schema '|| QUOTE_IDENT(pgn.nspname) ||' owner to ' || $2
			      FROM pg_namespace pgn,
				   pg_user pgu
			      WHERE pgn.nspowner = pgu.usesysid
			  UNION ALL
			      -- Tables or Views owned by the user
			      SELECT pgu.usesysid,
			      'alter table ' || QUOTE_IDENT(nc.nspname) || '.' || QUOTE_IDENT(pgc.relname) || ' owner to ' || $2
			      FROM pg_class pgc,
				   pg_user pgu,
				   pg_namespace nc
			      WHERE pgc.relnamespace = nc.oid
			      AND   pgc.relkind IN ('r','v')
			      AND   pgu.usesysid = pgc.relowner
			      AND   nc.nspname NOT ILIKE 'pg\_temp\_%'
			)
			OWNER("userid", "ddl")
			WHERE owner.userid = $1;`

	rows, err := tx.Query(reassignOwnerGenerator, useSysID, pq.QuoteIdentifier(newOwnerName))
	if err != nil {
		return err
	}

	var reassignStatements []string
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			_ = rows.Close()
			return err
		}

		reassignStatements = append(reassignStatements, statement)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, statement := range reassignStatements {
		if _, err := tx.Exec(statement); err != nil {
			log.Printf("error: %#v", err)
			return err
		}
	}

	rows, err = tx.Query("SELECT nspname FROM pg_namespace WHERE nspowner != 1 OR nspname = 'public'")
	if err != nil {
		return err
	}

	var schemaNames []string

	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			_ = rows.Close()
			return err
		}
		schemaNames = append(schemaNames, schemaName)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, schemaName := range schemaNames {
		if _, err := tx.Exec(fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(userName))); err != nil {
			return err
		}

		if _, err := tx.Exec(fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON TABLES FROM %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(userName))); err != nil {
			return err
		}

	}

	if _, err := tx.Exec(fmt.Sprintf("DROP USER %s", pq.QuoteIdentifier(userName))); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
		//return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

// parseUserSessionParameters parses pg_user.useconfig (a text[] of
// "param=value" strings) into session_parameters state. The provider is the
// exclusive owner of a user's session parameters: any parameter present on
// the user but absent from config is adopted into state and RESET on the
// next apply, so config remains the single source of truth. Redshift folds
// parameter names to lowercase, so names are normalized the same way.
func parseUserSessionParameters(userConfig []string) map[string]string {
	sessionParameters := make(map[string]string, len(userConfig))
	for _, entry := range userConfig {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			log.Printf("[WARN] ignoring unparseable useconfig entry %q", entry)
			continue
		}
		name := strings.ToLower(parts[0])
		sessionParameters[name] = unquoteUseConfigValue(parts[1])
	}
	return sessionParameters
}

// unquoteUseConfigValue reverses the quoting Redshift applies to useconfig
// values. Values containing characters such as spaces or commas (for example a
// multi-schema search_path) are stored wrapped in double quotes with any
// embedded double quotes doubled. Because the provider always writes values via
// `ALTER USER ... SET <param> = '<value>'`, reversing that serialization lets
// such values round-trip without a perpetual diff. Values without special
// characters are stored verbatim and pass through unchanged.
func unquoteUseConfigValue(value string) string {
	if len(value) >= 2 && strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
		value = strings.ReplaceAll(value, `""`, `"`)
	}
	return value
}

const redshiftDataApiInfinityDateString = "2038-01-19 03:14:04"

var redshiftDataApiDatetimeRegexp = regexp.MustCompile(`^\d+-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
var correctDatetimeRegexp = regexp.MustCompile(`^\d+-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00$`)

func validateAndAdjustValidUntil(validUntil string) (string, error) {
	if validUntil == "infinity" {
		return validUntil, nil
	} else if validUntil == redshiftDataApiInfinityDateString {
		// The Redshift Data API translates the `infinity` to a date in 2038 (see https://en.wikipedia.org/wiki/Year_2038_problem)
		return "infinity", nil
	} else if redshiftDataApiDatetimeRegexp.MatchString(validUntil) {
		// The Redshift Data API returns the datetime without the timezone offset, so we need to add it
		validUntil += "+00"
	}
	if !correctDatetimeRegexp.MatchString(validUntil) {
		return "", fmt.Errorf(`received invalid date format for valid_until: %q, expected format is "YYYY-MM-DD HH:MM:SS+00"`, validUntil)
	}
	return validUntil, nil
}

// stringMapFromMap reads a map of strings out of a framework value.
func stringMapFromMap(ctx context.Context, values types.Map, diagnostics *diag.Diagnostics) map[string]string {
	if values.IsNull() || values.IsUnknown() {
		return nil
	}
	result := map[string]string{}
	diagnostics.Append(values.ElementsAs(ctx, &result, false)...)
	return result
}
