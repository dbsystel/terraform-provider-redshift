package redshift

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/lib/pq"
)

// frameworkClient is embedded by every framework resource and data source. It holds the
// client handed out by the provider's Configure and opens connections from it, which is
// what ResourceFunc does for the parts still served by the SDK provider.
type frameworkClient struct {
	client *Client
}

func (c *frameworkClient) configureDataSource(req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	client, ok := req.ProviderData.(*Client)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data", "The provider did not hand out a Redshift client.")
		return
	}
	c.client = client
}

func (c *frameworkClient) configureResource(req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	client, ok := req.ProviderData.(*Client)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data", "The provider did not hand out a Redshift client.")
		return
	}
	c.client = client
}

// connect opens a connection, reporting a failure as a diagnostic. It returns nil when the
// connection could not be established, in which case the caller must return.
func (c *frameworkClient) connect(diagnostics *diag.Diagnostics) *DBConnection {
	db, err := c.client.Connect()
	if err != nil {
		diagnostics.AddError("Unable to connect to Redshift", err.Error())
		return nil
	}
	return db
}

// regexDoesNotMatchValidator is the counterpart of stringvalidator.RegexMatches, which
// terraform-plugin-framework-validators has no negated form of.
type regexDoesNotMatchValidator struct {
	regexp  *regexp.Regexp
	message string
}

func regexDoesNotMatch(expression *regexp.Regexp, message string) validator.String {
	return regexDoesNotMatchValidator{regexp: expression, message: message}
}

func (v regexDoesNotMatchValidator) Description(_ context.Context) string {
	return v.message
}

func (v regexDoesNotMatchValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (v regexDoesNotMatchValidator) ValidateString(ctx context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if v.regexp.MatchString(req.ConfigValue.ValueString()) {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid Attribute Value", fmt.Sprintf("%s: %s", v.Description(ctx), req.ConfigValue.ValueString()))
	}
}

// normalizeStringPlanModifier stores a normalized form of the configured value, which is
// what the SDK resources do with a StateFunc.
type normalizeStringPlanModifier struct {
	normalize func(string) string
}

func normalizeString(normalize func(string) string) planmodifier.String {
	return normalizeStringPlanModifier{normalize: normalize}
}

func (m normalizeStringPlanModifier) Description(_ context.Context) string {
	return "stores a normalized form of the configured value"
}

func (m normalizeStringPlanModifier) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m normalizeStringPlanModifier) PlanModifyString(_ context.Context, req planmodifier.StringRequest, resp *planmodifier.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	resp.PlanValue = types.StringValue(m.normalize(req.ConfigValue.ValueString()))
}

// resourceRetryAttempts is how often an operation is retried before giving up.
const resourceRetryAttempts = 10

// retryOnPQErrors retries an operation that failed with one of the Redshift errors that
// concurrent operations produce spuriously. It is the framework counterpart of
// ResourceRetryOnPQErrors, and unlike it reports the last error instead of swallowing it.
func retryOnPQErrors(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < resourceRetryAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		var pqErr *pq.Error
		if !errors.As(err, &pqErr) || !isRetryablePQError(string(pqErr.Code)) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
	return err
}

// normalizeSetPlanModifier is normalizeString for a set of strings: the framework has no
// per-element hook, so the whole set is rewritten.
type normalizeSetPlanModifier struct {
	normalize func(string) string
}

func normalizeSet(normalize func(string) string) planmodifier.Set {
	return normalizeSetPlanModifier{normalize: normalize}
}

func (m normalizeSetPlanModifier) Description(_ context.Context) string {
	return "stores a normalized form of the configured values"
}

func (m normalizeSetPlanModifier) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m normalizeSetPlanModifier) PlanModifySet(ctx context.Context, req planmodifier.SetRequest, resp *planmodifier.SetResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}

	var values []string
	resp.Diagnostics.Append(req.ConfigValue.ElementsAs(ctx, &values, false)...)
	if resp.Diagnostics.HasError() {
		return
	}
	for i, value := range values {
		values[i] = m.normalize(value)
	}

	normalized, diags := types.SetValueFrom(ctx, types.StringType, values)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.PlanValue = normalized
}

// stringsFromSet reads a set of strings out of a framework value.
func stringsFromSet(ctx context.Context, set types.Set, diagnostics *diag.Diagnostics) []string {
	if set.IsNull() || set.IsUnknown() {
		return nil
	}
	var values []string
	diagnostics.Append(set.ElementsAs(ctx, &values, false)...)
	return values
}

// requiresReplaceIfListSizeChangedModifier replaces the resource when a block is added or
// removed, which is what forceNewIfListSizeChanged does for the SDK resources.
func requiresReplaceIfListSizeChanged() planmodifier.List {
	return listplanmodifier.RequiresReplaceIf(
		func(_ context.Context, req planmodifier.ListRequest, resp *listplanmodifier.RequiresReplaceIfFuncResponse) {
			if req.StateValue.IsNull() || req.PlanValue.IsNull() || req.PlanValue.IsUnknown() {
				return
			}
			resp.RequiresReplace = len(req.StateValue.Elements()) != len(req.PlanValue.Elements())
		},
		"Replace the resource when blocks are added or removed",
		"Replace the resource when blocks are added or removed",
	)
}

// intersectStrings returns the values present in both slices, keeping the order of the
// first one.
func intersectStrings(first, second []string) []string {
	shared := []string{}
	for _, value := range first {
		if slices.Contains(second, value) {
			shared = append(shared, value)
		}
	}
	return shared
}

// scaleInt64 stores the configured value multiplied by factor, which is how the SDK
// resources kept a value in a different unit in state than in the configuration.
func scaleInt64(factor int64) planmodifier.Int64 {
	return scaleInt64PlanModifier{factor: factor}
}

type scaleInt64PlanModifier struct {
	factor int64
}

func (m scaleInt64PlanModifier) Description(_ context.Context) string {
	return fmt.Sprintf("stores the configured value multiplied by %d", m.factor)
}

func (m scaleInt64PlanModifier) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m scaleInt64PlanModifier) PlanModifyInt64(_ context.Context, req planmodifier.Int64Request, resp *planmodifier.Int64Response) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	resp.PlanValue = types.Int64Value(req.ConfigValue.ValueInt64() * m.factor)
}

// ignoreChangesAfterCreate keeps the value stored at creation time, for arguments that
// only have an effect while the resource is created and cannot be read back.
func ignoreChangesAfterCreate() planmodifier.Bool {
	return ignoreChangesAfterCreatePlanModifier{}
}

type ignoreChangesAfterCreatePlanModifier struct{}

func (m ignoreChangesAfterCreatePlanModifier) Description(_ context.Context) string {
	return "keeps the value stored when the resource was created"
}

func (m ignoreChangesAfterCreatePlanModifier) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m ignoreChangesAfterCreatePlanModifier) PlanModifyBool(_ context.Context, req planmodifier.BoolRequest, resp *planmodifier.BoolResponse) {
	if req.StateValue.IsNull() {
		return
	}
	resp.PlanValue = req.StateValue
}

// stringsFromList reads a list of strings out of a framework value.
func stringsFromList(ctx context.Context, list types.List, diagnostics *diag.Diagnostics) []string {
	if list.IsNull() || list.IsUnknown() {
		return nil
	}
	var values []string
	diagnostics.Append(list.ElementsAs(ctx, &values, false)...)
	return values
}

// listFromStrings turns a slice into a framework list, using a null list for no values.
func listFromStrings(ctx context.Context, values []string, diagnostics *diag.Diagnostics) types.List {
	if len(values) == 0 {
		return types.ListNull(types.StringType)
	}
	list, diags := types.ListValueFrom(ctx, types.StringType, values)
	diagnostics.Append(diags...)
	return list
}
