package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

type fakeGrantRevokeExecutor struct {
	errs    map[string]error
	queries []string
}

func (e *fakeGrantRevokeExecutor) Exec(query string, _ ...interface{}) (sql.Result, error) {
	e.queries = append(e.queries, query)
	return nil, e.errs[query]
}

func testGrantResourceData(t *testing.T, objectType string, objects []string) *schema.ResourceData {
	t.Helper()

	raw := map[string]interface{}{
		grantGroupAttr:      "test_group",
		grantObjectTypeAttr: objectType,
		grantPrivilegesAttr: []interface{}{"usage"},
	}
	if objectType == "database" {
		raw[grantDatabaseAttr] = "test_database"
	}
	if objectType == "schema" || objectType == "table" || objectType == "function" || objectType == "procedure" {
		raw[grantSchemaAttr] = "test_schema"
	}
	if len(objects) > 0 {
		values := make([]interface{}, len(objects))
		for i, object := range objects {
			values[i] = object
		}
		raw[grantObjectsAttr] = values
	}
	return schema.TestResourceDataRaw(t, redshiftGrant().Schema, raw)
}

func TestIsMissingGrantParticipantPQError(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		target grantRevokeTarget
		want   bool
	}{
		{"database", "3D000", grantRevokeTarget{objectType: "database"}, true},
		{"schema", "3F000", grantRevokeTarget{objectType: "schema"}, true},
		{"all tables schema", "3F000", grantRevokeTarget{objectType: "table"}, true},
		{"named table", "42P01", grantRevokeTarget{objectType: "table", named: true}, true},
		{"named function", "42883", grantRevokeTarget{objectType: "function", named: true}, true},
		{"named procedure", "42883", grantRevokeTarget{objectType: "procedure", named: true}, true},
		{"language", "42704", grantRevokeTarget{objectType: "language", named: true}, true},
		{"missing principal", "42704", grantRevokeTarget{objectType: "database"}, true},
		{"database code on schema", "3D000", grantRevokeTarget{objectType: "schema"}, false},
		{"relation code on all tables", "42P01", grantRevokeTarget{objectType: "table"}, false},
		{"function code on table", "42883", grantRevokeTarget{objectType: "table", named: true}, false},
		{"permission denied", "42501", grantRevokeTarget{objectType: "schema"}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := fmt.Errorf("revoke: %w", &pq.Error{Code: pq.ErrorCode(test.code)})
			statement := grantRevokeStatement{query: "REVOKE", target: test.target}
			if got := isMissingGrantParticipantError(err, "postgres", statement); got != test.want {
				t.Fatalf("isMissingGrantParticipantError() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestIsMissingGrantParticipantDataAPIError(t *testing.T) {
	query := `REVOKE ALL PRIVILEGES ON SCHEMA "test_schema" FROM GROUP "test_group"`
	baseStatement := grantRevokeStatement{query: query}
	tests := []struct {
		name    string
		payload string
		target  grantRevokeTarget
		driver  string
		query   string
		want    bool
	}{
		{"database", `Database "test_database" does not exist.`, grantRevokeTarget{objectType: "database"}, redshiftDataDriverName, query, true},
		{"schema capitalization", `ERROR: schema "test_schema" does not exist`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, true},
		{"all tables schema", `Schema "test_schema" does not exist.`, grantRevokeTarget{objectType: "table"}, redshiftDataDriverName, query, true},
		{"named relation", `relation "test_schema.table_a" does not exist`, grantRevokeTarget{objectType: "table", named: true}, redshiftDataDriverName, query, true},
		{"named function", `function test_schema.fn() does not exist`, grantRevokeTarget{objectType: "function", named: true}, redshiftDataDriverName, query, true},
		{"named procedure", `procedure test_schema.proc() does not exist.`, grantRevokeTarget{objectType: "procedure", named: true}, redshiftDataDriverName, query, true},
		{"procedure reported as function", `function test_schema.proc() does not exist.`, grantRevokeTarget{objectType: "procedure", named: true}, redshiftDataDriverName, query, true},
		{"language", `Language "plpgsql" does not exist.`, grantRevokeTarget{objectType: "language", named: true}, redshiftDataDriverName, query, true},
		{"user", `user "test_user" does not exist`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, true},
		{"group", `Group "test_group" does not exist.`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, true},
		{"role", `role "test_role" does not exist`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, true},
		{"wrong driver", `Schema "test_schema" does not exist.`, grantRevokeTarget{objectType: "schema"}, "postgres", query, false},
		{"wrong statement wrapper", `Schema "test_schema" does not exist.`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, "REVOKE OTHER", false},
		{"relation for all tables", `relation "test_schema.table_a" does not exist`, grantRevokeTarget{objectType: "table"}, redshiftDataDriverName, query, false},
		{"database for schema", `Database "test_database" does not exist.`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, false},
		{"generic does not exist", `object "test" does not exist`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, false},
		{"authorization error", `permission denied for schema test_schema`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, false},
		{"syntax error", `syntax error at or near "SCHEMA"`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, false},
		{"connection error", `connection reset by peer`, grantRevokeTarget{objectType: "schema"}, redshiftDataDriverName, query, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement := baseStatement
			statement.query = test.query
			statement.target = test.target
			err := fmt.Errorf("query failed (sql: %q): %s", query, test.payload)
			if got := isMissingGrantParticipantError(err, test.driver, statement); got != test.want {
				t.Fatalf("isMissingGrantParticipantError() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCreateGrantsDeleteRevokeStatements(t *testing.T) {
	tests := []struct {
		name        string
		objectType  string
		objects     []string
		wantCount   int
		wantNamed   bool
		queryPhrase string
	}{
		{"database", "database", nil, 1, false, "ON DATABASE"},
		{"schema", "schema", nil, 1, false, "ON SCHEMA"},
		{"all tables", "table", nil, 1, false, "ON ALL TABLES IN SCHEMA"},
		{"named tables", "table", []string{"table_a", "table_b"}, 2, true, "ON TABLE"},
		{"all functions", "function", nil, 1, false, "ON ALL FUNCTIONS IN SCHEMA"},
		{"named functions", "function", []string{"fn_a()", "fn_b()"}, 2, true, "ON FUNCTION"},
		{"all procedures", "procedure", nil, 1, false, "ON ALL PROCEDURES IN SCHEMA"},
		{"named procedures", "procedure", []string{"proc_a()", "proc_b()"}, 2, true, "ON PROCEDURE"},
		{"languages", "language", []string{"plpgsql", "sql"}, 2, true, "ON LANGUAGE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statements := createGrantsDeleteRevokeStatements(testGrantResourceData(t, test.objectType, test.objects), "test_database")
			if len(statements) != test.wantCount {
				t.Fatalf("got %d statements, want %d", len(statements), test.wantCount)
			}
			for _, statement := range statements {
				if statement.target.named != test.wantNamed || !strings.Contains(statement.query, test.queryPhrase) {
					t.Fatalf("unexpected delete statement: %#v", statement)
				}
			}
		})
	}
}

func TestCreateGrantsRevokeQueryKeepsNamedTargetsBatched(t *testing.T) {
	d := testGrantResourceData(t, "table", []string{"table_a", "table_b"})
	query := createGrantsRevokeQuery(d, "test_database")
	if !strings.Contains(query, `"test_schema"."table_a"`) || !strings.Contains(query, `"test_schema"."table_b"`) || !strings.Contains(query, ",") {
		t.Fatalf("createGrantsRevokeQuery() did not preserve the batched query: %s", query)
	}
}

func TestRevokeGrantsForDeleteBestEffort(t *testing.T) {
	statements := []grantRevokeStatement{
		{query: "missing", target: grantRevokeTarget{objectType: "schema"}},
		{query: "permission", target: grantRevokeTarget{objectType: "table", name: "table_a", named: true}},
		{query: "success", target: grantRevokeTarget{objectType: "table", name: "table_b", named: true}},
	}
	executor := &fakeGrantRevokeExecutor{errs: map[string]error{
		"missing":    &pq.Error{Code: pqErrorCodeInvalidSchemaName},
		"permission": fmt.Errorf("permission denied"),
	}}

	err := revokeGrantsForDelete(executor, "postgres", statements)
	if err == nil || !strings.Contains(err.Error(), "named table") || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("revokeGrantsForDelete() error = %v, want contextual permission error", err)
	}
	if got := strings.Join(executor.queries, ","); got != "missing,permission,success" {
		t.Fatalf("revokeGrantsForDelete() executed %q, want every statement", got)
	}
}

func TestRevokeGrantsForDeleteJoinsUnexpectedErrors(t *testing.T) {
	firstFailure := errors.New("first failure")
	secondFailure := errors.New("second failure")
	statements := []grantRevokeStatement{
		{query: "first", target: grantRevokeTarget{objectType: "schema"}},
		{query: "second", target: grantRevokeTarget{objectType: "database"}},
	}
	executor := &fakeGrantRevokeExecutor{errs: map[string]error{
		"first":  firstFailure,
		"second": secondFailure,
	}}

	err := revokeGrantsForDelete(executor, "postgres", statements)
	if err == nil || !errors.Is(err, firstFailure) || !errors.Is(err, secondFailure) {
		t.Fatalf("revokeGrantsForDelete() error = %v, want joined errors", err)
	}
}
