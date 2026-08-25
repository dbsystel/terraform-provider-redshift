package redshift

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

const (
	dataShareNameAttr              = "name"
	dataShareOwnerAttr             = "owner"
	dataSharePublicAccessibleAttr  = "publicly_accessible"
	dataShareProducerAccountAttr   = "producer_account"
	dataShareProducerNamespaceAttr = "producer_namespace"
	dataShareCreatedAttr           = "created"
	dataShareSchemasAttr           = "schemas"
)

var (
	_ resource.Resource                = &datashareResource{}
	_ resource.ResourceWithConfigure   = &datashareResource{}
	_ resource.ResourceWithImportState = &datashareResource{}
)

func newDatashareResource() resource.Resource {
	return &datashareResource{}
}

type datashareResource struct {
	frameworkClient
}

type datashareResourceModel struct {
	ID                 types.String `tfsdk:"id"`
	Name               types.String `tfsdk:"name"`
	Owner              types.String `tfsdk:"owner"`
	PubliclyAccessible types.Bool   `tfsdk:"publicly_accessible"`
	ProducerAccount    types.String `tfsdk:"producer_account"`
	ProducerNamespace  types.String `tfsdk:"producer_namespace"`
	Created            types.String `tfsdk:"created"`
	Schemas            types.Set    `tfsdk:"schemas"`
}

func (r *datashareResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_datashare"
}

func (r *datashareResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: `
Defines a Redshift datashare. Datashares allows a Redshift cluster (the "consumer") to
read data stored in another Redshift cluster (the "producer"). For more information, see
https://docs.aws.amazon.com/redshift/latest/dg/datashare-overview.html

The redshift_datashare resource should be defined on the producer cluster.

Note: Data sharing is only supported on certain Redshift instance families,
such as RA3.
`,
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The datashare ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			dataShareNameAttr: schema.StringAttribute{
				Required:    true,
				Description: "The name of the datashare.",
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
					stringplanmodifier.RequiresReplace(),
				},
			},
			dataShareOwnerAttr: schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "The user who owns the datashare.",
				PlanModifiers: []planmodifier.String{
					normalizeString(strings.ToLower),
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			dataSharePublicAccessibleAttr: schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: "Specifies whether the datashare can be shared to clusters that are publicly accessible. Default is `false`.",
			},
			dataShareProducerAccountAttr: schema.StringAttribute{
				Computed:    true,
				Description: "The ID for the datashare producer account.",
			},
			dataShareProducerNamespaceAttr: schema.StringAttribute{
				Computed:    true,
				Description: "The unique cluster identifier for the datashare producer cluster.",
			},
			dataShareCreatedAttr: schema.StringAttribute{
				Computed:    true,
				Description: "The date when datashare was created",
			},
			dataShareSchemasAttr: schema.SetAttribute{
				Optional:    true,
				ElementType: types.StringType,
				Description: "Defines which schemas are exposed to the data share.",
				PlanModifiers: []planmodifier.Set{
					normalizeSet(strings.ToLower),
				},
			},
		},
	}
}

func (r *datashareResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.configureResource(req, resp)
}

func (r *datashareResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *datashareResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var model datashareResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	schemas := stringsFromSet(ctx, model.Schemas, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	owner := ""
	if !model.Owner.IsNull() && !model.Owner.IsUnknown() {
		owner = model.Owner.ValueString()
	}

	shareID, err := createDatashare(db, model.Name.ValueString(), owner, model.PubliclyAccessible.ValueBool(), schemas)
	if err != nil {
		resp.Diagnostics.AddError("Unable to create the datashare", err.Error())
		return
	}

	model.ID = types.StringValue(shareID)
	if !r.refresh(ctx, db, &model, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func createDatashare(db *DBConnection, shareName, owner string, publiclyAccessible bool, schemas []string) (string, error) {
	tx, err := startTransaction(db.client)
	if err != nil {
		return "", err
	}
	defer deferredRollback(tx)

	query := fmt.Sprintf("CREATE DATASHARE %s SET PUBLICACCESSIBLE = %t", pq.QuoteIdentifier(shareName), publiclyAccessible)
	log.Printf("[DEBUG] %s\n", query)
	if _, err := tx.Exec(query); err != nil {
		return "", err
	}

	var shareID string
	query = "SELECT share_id FROM SVV_DATASHARES WHERE share_type = 'OUTBOUND' AND share_name = $1"
	log.Printf("[DEBUG] %s, $1=%s\n", query, strings.ToLower(shareName))
	if err := tx.QueryRow(query, strings.ToLower(shareName)).Scan(&shareID); err != nil {
		return "", err
	}

	if owner != "" {
		query = fmt.Sprintf("ALTER DATASHARE %s OWNER TO %s", pq.QuoteIdentifier(strings.ToLower(shareName)), pq.QuoteIdentifier(strings.ToLower(owner)))
		log.Printf("[DEBUG] %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return "", err
		}
	}

	for _, schemaName := range schemas {
		if err := addSchemaToDatashare(tx, shareName, schemaName); err != nil {
			return "", err
		}
	}

	if err = tx.Commit(); err != nil {
		return "", fmt.Errorf("could not commit transaction: %w", err)
	}

	return shareID, nil
}

func addSchemaToDatashare(tx *sql.Tx, shareName string, schemaName string) error {
	err := resourceRedshiftDatashareAddSchema(tx, shareName, schemaName)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareAddAllTables(tx, shareName, schemaName)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareAddAllFunctions(tx, shareName, schemaName)
	return err
}

func resourceRedshiftDatashareAddSchema(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	if err != nil {
		// if the schema is already in the datashare we get a "duplicate schema" error code. This is fine.
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			if string(pqErr.Code) == pqErrorCodeDuplicateSchema {
				log.Printf("[WARN] Schema %s already exists in datashare %s\n", schemaName, shareName)
			} else {
				return err
			}
		} else {
			return err
		}
	}
	query = fmt.Sprintf("ALTER DATASHARE %s SET INCLUDENEW = TRUE FOR SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err = tx.Exec(query)
	return err
}

func resourceRedshiftDatashareAddAllFunctions(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD ALL FUNCTIONS IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareAddAllTables(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s ADD ALL TABLES IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func removeSchemaFromDatashare(tx *sql.Tx, shareName string, schemaName string) error {
	err := resourceRedshiftDatashareRemoveAllFunctions(tx, shareName, schemaName)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareRemoveAllTables(tx, shareName, schemaName)
	if err != nil {
		return err
	}
	err = resourceRedshiftDatashareRemoveSchema(tx, shareName, schemaName)
	return err
}

func resourceRedshiftDatashareRemoveAllFunctions(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE ALL FUNCTIONS IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveAllTables(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE ALL TABLES IN SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	return err
}

func resourceRedshiftDatashareRemoveSchema(tx *sql.Tx, shareName string, schemaName string) error {
	query := fmt.Sprintf("ALTER DATASHARE %s REMOVE SCHEMA %s", pq.QuoteIdentifier(shareName), pq.QuoteIdentifier(schemaName))
	log.Printf("[DEBUG] %s\n", query)
	_, err := tx.Exec(query)
	if err != nil {
		// if the schema is not already in the datashare we get a "datashare does not contain schema" error code. This is fine.
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			if string(pqErr.Code) == pqErrorCodeInvalidSchemaName {
				log.Printf("[WARN] Schema %s does not exist in datashare %s\n", schemaName, shareName)
			} else {
				return err
			}
		}
	}
	return nil
}

func (r *datashareResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var model datashareResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if !r.refresh(ctx, db, &model, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// refresh fills the model from the cluster. It reports whether the caller may go on.
func (r *datashareResource) refresh(ctx context.Context, db *DBConnection, model *datashareResourceModel, diagnostics *diag.Diagnostics) bool {
	var shareName, owner, producerAccount, producerNamespace, created string
	var publicAccessible bool

	tx, err := startTransaction(db.client)
	if err != nil {
		diagnostics.AddError("Unable to read the datashare", err.Error())
		return false
	}
	defer deferredRollback(tx)

	query := `
	SELECT
		TRIM(svv_datashares.share_name),
		TRIM(pg_user.usename),
		svv_datashares.is_publicaccessible,
		TRIM(COALESCE(svv_datashares.producer_account, '')),
		TRIM(COALESCE(svv_datashares.producer_namespace, '')),
		REPLACE(TO_CHAR(svv_datashares.createdate, 'YYYY-MM-DD HH24:MI:SS'), ' ', 'T') || 'Z'
	FROM svv_datashares
	LEFT JOIN pg_user ON svv_datashares.share_owner = pg_user.usesysid
	WHERE share_type = 'OUTBOUND'
	AND share_id = $1`
	log.Printf("[DEBUG] %s, $1=%s\n", query, model.ID.ValueString())
	if err := tx.QueryRow(query, model.ID.ValueString()).Scan(&shareName, &owner, &publicAccessible, &producerAccount, &producerNamespace, &created); err != nil {
		diagnostics.AddError("Unable to read the datashare", err.Error())
		return false
	}

	schemas, err := readDatashareSchemas(tx, shareName)
	if err != nil {
		diagnostics.AddError("Unable to read the datashare schemas", err.Error())
		return false
	}

	if err = tx.Commit(); err != nil {
		diagnostics.AddError("Unable to read the datashare", err.Error())
		return false
	}

	model.Name = types.StringValue(shareName)
	model.Owner = types.StringValue(owner)
	model.PubliclyAccessible = types.BoolValue(publicAccessible)
	model.ProducerAccount = types.StringValue(producerAccount)
	model.ProducerNamespace = types.StringValue(producerNamespace)
	model.Created = types.StringValue(created)

	// Schemas that the configuration does not manage stay unset, so that omitting the
	// argument does not plan a change on every run.
	if len(schemas) > 0 || !model.Schemas.IsNull() {
		shared, diags := types.SetValueFrom(ctx, types.StringType, schemas)
		diagnostics.Append(diags...)
		if diagnostics.HasError() {
			return false
		}
		model.Schemas = shared
	}

	return true
}

func readDatashareSchemas(tx *sql.Tx, shareName string) ([]string, error) {
	query := `
	SELECT
		object_name
	FROM svv_datashare_objects
	WHERE share_type = 'OUTBOUND'
	AND object_type = 'schema'
	AND share_name = $1
`
	log.Printf("[DEBUG] %s, $1=%s\n", query, shareName)
	rows, err := tx.Query(query, shareName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err = rows.Scan(&schemaName); err != nil {
			return nil, err
		}
		schemas = append(schemas, schemaName)
	}
	return schemas, rows.Err()
}

func (r *datashareResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state datashareResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	oldSchemas := stringsFromSet(ctx, state.Schemas, &resp.Diagnostics)
	newSchemas := stringsFromSet(ctx, plan.Schemas, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := updateDatashare(plan, state, db, oldSchemas, newSchemas); err != nil {
		resp.Diagnostics.AddError("Unable to update the datashare", err.Error())
		return
	}

	plan.ID = state.ID
	if !r.refresh(ctx, db, &plan, &resp.Diagnostics) {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func updateDatashare(plan, state datashareResourceModel, db *DBConnection, oldSchemas, newSchemas []string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	shareName := plan.Name.ValueString()

	if !plan.Owner.IsUnknown() && plan.Owner.ValueString() != state.Owner.ValueString() {
		newOwner := "CURRENT_USER"
		if plan.Owner.ValueString() != "" {
			newOwner = pq.QuoteIdentifier(plan.Owner.ValueString())
		}
		query := fmt.Sprintf("ALTER DATASHARE %s OWNER TO %s", pq.QuoteIdentifier(shareName), newOwner)
		log.Printf("[DEBUG] %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating datashare OWNER: %w", err)
		}
	}

	if plan.PubliclyAccessible.ValueBool() != state.PubliclyAccessible.ValueBool() {
		query := fmt.Sprintf("ALTER DATASHARE %s SET PUBLICACCESSIBLE %t", pq.QuoteIdentifier(shareName), plan.PubliclyAccessible.ValueBool())
		log.Printf("[DEBUG] %s\n", query)
		if _, err := tx.Exec(query); err != nil {
			return fmt.Errorf("error updating datashare PUBLICACCESSBILE: %w", err)
		}
	}

	removed, added := diffStrings(oldSchemas, newSchemas)
	for _, schemaName := range added {
		if err := addSchemaToDatashare(tx, shareName, schemaName); err != nil {
			return err
		}
	}
	for _, schemaName := range removed {
		if err := removeSchemaFromDatashare(tx, shareName, schemaName); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}

func (r *datashareResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var model datashareResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.connect(&resp.Diagnostics)
	if db == nil {
		return
	}

	if err := deleteDatashare(db, model.ID.ValueString()); err != nil {
		resp.Diagnostics.AddError("Unable to delete the datashare", err.Error())
	}
}

func deleteDatashare(db *DBConnection, shareID string) error {
	tx, err := startTransaction(db.client)
	if err != nil {
		return err
	}
	defer deferredRollback(tx)

	var shareName string
	query := "SELECT share_name FROM svv_datashares WHERE share_type='OUTBOUND' AND share_id=$1"
	if err := tx.QueryRow(query, shareID).Scan(&shareName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("[WARN] data share with id %s does not exist.\n", shareID)
			return nil
		}
		return err
	}
	query = fmt.Sprintf("DROP DATASHARE %s", pq.QuoteIdentifier(shareName))
	log.Printf("[DEBUG] %s\n", query)
	if _, err = tx.Exec(query); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	return nil
}
