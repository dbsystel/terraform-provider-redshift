package redshift

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const databaseNameAttr = "name"
const databaseOwnerAttr = "owner"
const databaseConnLimitAttr = "connection_limit"
const databaseDatashareSourceAttr = "datashare_source"
const databaseDatashareSourceShareNameAttr = "share_name"
const databaseDatashareSourceNamespaceAttr = "namespace"
const databaseDatashareSourceAccountAttr = "account_id"
const databaseDatashareSourceWithPermissions = "with_permissions"
const databaseZeroETLIntegrationAttr = "zeroetl_integration"
const databaseZeroETLIntegrationIdAttr = "integration_id"

var (
	_ resource.Resource                = &databaseResource{}
	_ resource.ResourceWithConfigure   = &databaseResource{}
	_ resource.ResourceWithImportState = &databaseResource{}
)

func newDatabaseResource() resource.Resource {
	return &databaseResource{}
}

type databaseResource struct {
	frameworkClient
}

type databaseResourceModel struct {
	ID                 types.String                      `tfsdk:"id"`
	Name               types.String                      `tfsdk:"name"`
	Owner              types.String                      `tfsdk:"owner"`
	ConnectionLimit    types.Int64                       `tfsdk:"connection_limit"`
	DatashareSource    []databaseDatashareSourceModel    `tfsdk:"datashare_source"`
	ZeroETLIntegration []databaseZeroETLIntegrationModel `tfsdk:"zeroetl_integration"`
}

type databaseDatashareSourceModel struct {
	ShareName       types.String `tfsdk:"share_name"`
	Namespace       types.String `tfsdk:"namespace"`
	AccountID       types.String `tfsdk:"account_id"`
	WithPermissions types.Bool   `tfsdk:"with_permissions"`
}

type databaseZeroETLIntegrationModel struct {
	IntegrationID types.String `tfsdk:"integration_id"`
}

func (r *databaseResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

func (r *databaseResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `Defines a local database.`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The database ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			databaseNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "Name of the database",
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
				},
			},
			databaseOwnerAttr: schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Owner of the database, usually the user who created it",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			databaseConnLimitAttr: schema.Int64Attribute{
				Optional:    true,
				Computed:    true,
				Default:     int64default.StaticInt64(-1),
				Description: "The maximum number of concurrent connections that can be made to this database. A value of -1 means no limit.",
				Validators: []validator.Int64{
					int64validator.AtLeast(-1),
				},
			},
		},
		Blocks: map[string]schema.Block{
			databaseDatashareSourceAttr: schema.ListNestedBlock{
				Description: "Configuration for creating a database from a redshift datashare.",
				Validators: []validator.List{
					listvalidator.SizeAtMost(1),
					listvalidator.ConflictsWith(path.MatchRoot(databaseZeroETLIntegrationAttr)),
				},
				PlanModifiers: []planmodifier.List{
					requiresReplaceIfListSizeChanged(),
				},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						databaseDatashareSourceShareNameAttr: schema.StringAttribute{
							Required:    true,
							Description: "The name of the datashare on the producer cluster",
							PlanModifiers: []planmodifier.String{
								normalizeString(strings.ToLower),
								stringplanmodifier.RequiresReplace(),
							},
						},
						databaseDatashareSourceNamespaceAttr: schema.StringAttribute{
							Required:    true,
							Description: "The namespace (guid) of the producer cluster",
							PlanModifiers: []planmodifier.String{
								normalizeString(strings.ToLower),
								stringplanmodifier.RequiresReplace(),
							},
						},
						databaseDatashareSourceAccountAttr: schema.StringAttribute{
							Optional:    true,
							Computed:    true,
							Description: "The AWS account ID of the producer cluster.",
							Validators: []validator.String{
								stringvalidator.RegexMatches(awsAccountIdRegexp, "AWS account id must be a 12-digit number"),
							},
							PlanModifiers: []planmodifier.String{
								stringplanmodifier.UseStateForUnknown(),
								stringplanmodifier.RequiresReplace(),
							},
						},
						databaseDatashareSourceWithPermissions: schema.BoolAttribute{
							Optional:    true,
							Computed:    true,
							Default:     booldefault.StaticBool(false),
							Description: "Whether the database requires object-level permissions to access individual database objects",
							PlanModifiers: []planmodifier.Bool{
								boolplanmodifier.RequiresReplace(),
							},
						},
					},
				},
			},
			databaseZeroETLIntegrationAttr: schema.ListNestedBlock{
				Description: "Configuration for creating a database from a zero ETL integration.",
				Validators: []validator.List{
					listvalidator.SizeAtMost(1),
				},
				PlanModifiers: []planmodifier.List{
					requiresReplaceIfListSizeChanged(),
				},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						databaseZeroETLIntegrationIdAttr: schema.StringAttribute{
							Required:    true,
							Description: "The unique identifier of the zero ETL integration",
							PlanModifiers: []planmodifier.String{
								stringplanmodifier.RequiresReplace(),
							},
						},
					},
				},
			},
		},
	}
}

func (r *databaseResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *databaseResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *databaseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model databaseResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	var (
		oid string
		err error
	)
	if len(model.DatashareSource) > 0 {
		oid, err = createDatabaseFromDatashare(db, model)
	} else {
		oid, err = createDatabase(db, model)
	}
	if err != nil {
		resp.Diagnostics.AddError("Unable to create the database", err.Error())
		return
	}

	model.ID = types.StringValue(oid)
	if !r.refresh(db, &model, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func createDatabaseFromDatashare(db *DBConnection, model databaseResourceModel) (string, error) {
	dbName := model.Name.ValueString()
	source := model.DatashareSource[0]

	query := fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName))
	if source.WithPermissions.ValueBool() {
		query = fmt.Sprintf("%s WITH PERMISSIONS", query)
	}
	query = fmt.Sprintf("%s FROM DATASHARE %s OF", query, pq.QuoteIdentifier(source.ShareName.ValueString()))
	if !source.AccountID.IsNull() && !source.AccountID.IsUnknown() {
		query = fmt.Sprintf("%s ACCOUNT '%s'", query, pqQuoteLiteral(source.AccountID.ValueString()))
	}
	query = fmt.Sprintf("%s NAMESPACE '%s'", query, pqQuoteLiteral(source.Namespace.ValueString()))

	if _, err := db.Exec(query); err != nil {
		return "", err
	}

	// eagerly get the resource ID in case the below statements fail for some reason
	oid, err := databaseOID(db, dbName)
	if err != nil {
		return "", err
	}

	// CREATE DATABASE isn't allowed to run inside a transaction, however ALTER DATABASE
	// can be
	tx, err := startTransaction(db.client)
	if err != nil {
		return oid, err
	}
	defer deferredRollback(tx)

	// CREATE DATABASE FROM DATASHARE... doesn't allow you to specify an owner or the
	// connection limit in the create statement, so both are set afterwards.
	if !model.Owner.IsNull() && !model.Owner.IsUnknown() {
		if _, err = tx.Exec(fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(model.Owner.ValueString()))); err != nil {
			return oid, err
		}
	}
	if _, err = tx.Exec(fmt.Sprintf("ALTER DATABASE %s CONNECTION LIMIT %d", pq.QuoteIdentifier(dbName), model.ConnectionLimit.ValueInt64())); err != nil {
		return oid, err
	}

	return oid, tx.Commit()
}

func createDatabase(db *DBConnection, model databaseResourceModel) (string, error) {
	dbName := model.Name.ValueString()
	query := fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName))

	if len(model.ZeroETLIntegration) > 0 {
		query = fmt.Sprintf("%s FROM INTEGRATION '%s'", query, pqQuoteLiteral(model.ZeroETLIntegration[0].IntegrationID.ValueString()))
	}
	if !model.Owner.IsNull() && !model.Owner.IsUnknown() {
		query = fmt.Sprintf("%s OWNER %s", query, pq.QuoteIdentifier(model.Owner.ValueString()))
	}
	query = fmt.Sprintf("%s CONNECTION LIMIT %d", query, model.ConnectionLimit.ValueInt64())

	log.Printf("[DEBUG] create database %s: %s\n", dbName, query)
	if _, err := db.Exec(query); err != nil {
		return "", err
	}

	return databaseOID(db, dbName)
}

func databaseOID(db *DBConnection, dbName string) (string, error) {
	var oid string
	query := "SELECT oid FROM pg_database WHERE datname = $1"
	log.Printf("[DEBUG] get oid from database: %s\n", query)
	if err := db.QueryRow(query, strings.ToLower(dbName)).Scan(&oid); err != nil {
		return "", err
	}
	return oid, nil
}

func (r *databaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model databaseResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if !r.refresh(db, &model, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// refresh fills the model from the cluster. It reports whether the caller may go on.
//
// zeroetl_integration is left as it is: the cluster does not report whether a database
// came from an integration, so the configured value is all there is. That does not
// survive an import, but it does keep the plan empty during the normal lifecycle.
func (r *databaseResource) refresh(db *DBConnection, model *databaseResourceModel, diagnostics *diag.Diagnostics) bool {
	var name, owner, connLimit, databaseType, shareName, producerAccount, producerNamespace string

	query := `SELECT
  TRIM(svv_redshift_databases.database_name),
  TRIM(pg_user_info.usename),
  COALESCE(pg_database_info.datconnlimit::text, 'UNLIMITED'),
	svv_redshift_databases.database_type,
  TRIM(COALESCE(svv_datashares.share_name, '')),
  TRIM(COALESCE(svv_datashares.producer_account, '')),
  TRIM(COALESCE(svv_datashares.producer_namespace, ''))
FROM
  svv_redshift_databases
LEFT JOIN pg_database_info
  ON svv_redshift_databases.database_name=pg_database_info.datname
LEFT JOIN pg_user_info
  ON pg_user_info.usesysid = svv_redshift_databases.database_owner
LEFT JOIN svv_datashares
	ON (svv_redshift_databases.database_name = svv_datashares.consumer_database AND svv_redshift_databases.database_type = 'shared' AND svv_datashares.share_type = 'INBOUND')
WHERE pg_database_info.datid = $1
`
	log.Printf("[DEBUG] read database: %s\n", query)
	err := db.QueryRow(query, model.ID.ValueString()).Scan(&name, &owner, &connLimit, &databaseType, &shareName, &producerAccount, &producerNamespace)
	if err != nil {
		diagnostics.AddError("Unable to read the database", err.Error())
		return false
	}

	connLimitNumber := -1
	if connLimit != "UNLIMITED" {
		if connLimitNumber, err = strconv.Atoi(connLimit); err != nil {
			diagnostics.AddError("Unable to read the database connection limit", err.Error())
			return false
		}
	}

	model.Name = types.StringValue(name)
	model.Owner = types.StringValue(owner)
	model.ConnectionLimit = types.Int64Value(int64(connLimitNumber))

	if databaseType == "shared" {
		// The cluster does not report whether the database was created with permissions,
		// so that part of the block stays as configured.
		withPermissions := types.BoolValue(false)
		if len(model.DatashareSource) > 0 {
			withPermissions = model.DatashareSource[0].WithPermissions
		}
		model.DatashareSource = []databaseDatashareSourceModel{{
			ShareName:       types.StringValue(shareName),
			Namespace:       types.StringValue(producerNamespace),
			AccountID:       types.StringValue(producerAccount),
			WithPermissions: withPermissions,
		}}
	} else {
		// An absent datashare_source block decodes as an empty list, so keep the
		// refreshed value empty rather than null to avoid a perpetual diff.
		model.DatashareSource = []databaseDatashareSourceModel{}
	}

	return true
}

func (r *databaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state databaseResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if err := updateDatabase(db, plan, state); err != nil {
		resp.Diagnostics.AddError("Unable to update the database", err.Error())
		return
	}

	plan.ID = state.ID
	if !r.refresh(db, &plan, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func updateDatabase(db *DBConnection, plan, state databaseResourceModel) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	databaseName := plan.Name.ValueString()

	if databaseName != state.Name.ValueString() {
		if databaseName == "" {
			return fmt.Errorf("error setting database name to an empty string")
		}
		query := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", pq.QuoteIdentifier(state.Name.ValueString()), pq.QuoteIdentifier(databaseName))
		log.Printf("[DEBUG] renaming database %s to %s: %s\n", state.Name.ValueString(), databaseName, query)
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating database NAME: %w", err)
		}
	}

	if !plan.Owner.IsUnknown() && plan.Owner.ValueString() != state.Owner.ValueString() {
		query := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", pq.QuoteIdentifier(databaseName), pq.QuoteIdentifier(plan.Owner.ValueString()))
		log.Printf("[DEBUG] changing database owner: %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}

	if plan.ConnectionLimit.ValueInt64() != state.ConnectionLimit.ValueInt64() {
		query := fmt.Sprintf("ALTER DATABASE %s CONNECTION LIMIT %d", pq.QuoteIdentifier(databaseName), plan.ConnectionLimit.ValueInt64())
		log.Printf("[DEBUG] changing database connection limit: %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (r *databaseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model databaseResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	databaseName := model.Name.ValueString()
	query := fmt.Sprintf("DROP DATABASE %s", pqQuoteLiteral(databaseName))
	log.Printf("[DEBUG] dropping database %s: %s\n", databaseName, query)
	if _, err := db.Exec(query); err != nil {
		resp.Diagnostics.AddError("Unable to delete the database", err.Error())
	}
}
