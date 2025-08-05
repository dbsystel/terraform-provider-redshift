package redshift

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	pqErrorCodeConcurrent        = "XX000"
	pqErrorCodeInvalidSchemaName = "3F000"
	pqErrorCodeDeadlock          = "40P01"
	pqErrorCodeFailedTransaction = "25P02"
	pqErrorCodeDuplicateSchema   = "42P06"

	pgErrorCodeInsufficientPrivileges = "42501"
)

// pqQuoteLiteral returns a string literal safe for inclusion in a PostgreSQL
// query as a parameter.  The resulting string still needs to be wrapped in
// single quotes in SQL (i.e. fmt.Sprintf(`'%s'`, pqQuoteLiteral("str"))).  See
// quote_literal_internal() in postgresql/backend/utils/adt/quote.c:77.
func pqQuoteLiteral(in string) string {
	in = strings.ReplaceAll(in, `\`, `\\`)
	in = strings.ReplaceAll(in, `'`, `''`)
	return in
}

func getGroupIDFromName(db *sql.DB, group string) (groupID int, err error) {
	err = db.QueryRow("SELECT grosysid FROM pg_group WHERE groname = $1", group).Scan(&groupID)
	return
}

func getUserIDFromName(db *sql.DB, user string) (userID int, err error) {
	err = db.QueryRow("SELECT usesysid FROM pg_user WHERE usename = $1", user).Scan(&userID)
	return
}

func getSchemaIDFromName(db *sql.DB, schema string) (schemaID int, err error) {
	err = db.QueryRow("SELECT oid FROM pg_namespace WHERE nspname = $1", schema).Scan(&schemaID)
	return
}

func ResourceFunc(fn func(*DBConnection, *schema.ResourceData) error) func(context.Context, *schema.ResourceData, interface{}) diag.Diagnostics {
	return func(_ context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
		client := meta.(*Client)

		db, err := client.Connect()
		if err != nil {
			return diag.FromErr(err)
		}

		return diag.FromErr(fn(db, d))
	}
}

func ResourceRetryOnPQErrors(fn func(*DBConnection, *schema.ResourceData) error) func(*DBConnection, *schema.ResourceData) error {
	return func(db *DBConnection, d *schema.ResourceData) error {
		for i := 0; i < 10; i++ {
			err := fn(db, d)
			if err == nil {
				return nil
			}

			var pqErr *pq.Error
			if !errors.As(err, &pqErr) || !isRetryablePQError(string(pqErr.Code)) {
				return err
			}

			time.Sleep(time.Duration(i+1) * time.Second)
		}
		return nil
	}
}

func isRetryablePQError(code string) bool {
	retryable := map[string]bool{
		pqErrorCodeConcurrent:        true,
		pqErrorCodeInvalidSchemaName: true,
		pqErrorCodeDeadlock:          true,
		pqErrorCodeFailedTransaction: true,
	}

	_, ok := retryable[code]
	return ok
}

func isPqErrorWithCode(err error, code string) bool {
	return string(err.(*pq.Error).Code) == code
}

func splitCsvAndTrim(raw string) ([]string, error) {
	if raw == "" {
		return []string{}, nil
	}
	reader := csv.NewReader(strings.NewReader(raw))
	rawSlice, err := reader.Read()
	if err != nil {
		return nil, err
	}
	var result []string
	for _, s := range rawSlice {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result, nil
}

func validatePrivileges(privileges []string, objectType string) bool {
	if objectType == "language" && len(privileges) == 0 {
		return false
	}
	for _, p := range privileges {
		switch strings.ToUpper(objectType) {
		case "SCHEMA":
			switch strings.ToUpper(p) {
			case "CREATE", "USAGE":
				continue
			default:
				return false
			}
		case "TABLE":
			switch strings.ToUpper(p) {
			case "SELECT", "UPDATE", "INSERT", "DELETE", "DROP", "REFERENCES", "RULE", "TRIGGER":
				continue
			default:
				return false
			}
		case "DATABASE":
			switch strings.ToUpper(p) {
			// USAGE is only available from databases created from datashares
			case "CREATE", "TEMPORARY", "USAGE":
				continue
			default:
				return false
			}
		case "PROCEDURE", "FUNCTION":
			switch strings.ToUpper(p) {
			case "EXECUTE":
				continue
			default:
				return false
			}
		case "LANGUAGE":
			switch strings.ToUpper(p) {
			case "USAGE":
				continue
			default:
				return false
			}
		default:
			return false
		}
	}

	return true
}

func appendIfTrue(condition bool, item string, list *[]string) {
	if condition {
		*list = append(*list, item)
	}
}

func setToPgIdentList(identifiers *schema.Set, prefix string) string {
	quoted := make([]string, identifiers.Len())
	for i, identifier := range identifiers.List() {
		if prefix == "" {
			quoted[i] = pq.QuoteIdentifier(identifier.(string))
		} else {
			quoted[i] = fmt.Sprintf("%s.%s", pq.QuoteIdentifier(prefix), pq.QuoteIdentifier(identifier.(string)))
		}
	}

	return strings.Join(quoted, ",")
}

// Quoted identifiers somehow does not work for grants/revokes on functions and procedures
func setToPgIdentListNotQuoted(identifiers *schema.Set, prefix string) string {
	quoted := make([]string, identifiers.Len())
	for i, identifier := range identifiers.List() {
		if prefix == "" {
			quoted[i] = identifier.(string)
		} else {
			quoted[i] = fmt.Sprintf("%s.%s", prefix, identifier.(string))
		}
	}

	return strings.Join(quoted, ",")
}

func stripArgumentsFromCallablesDefinitions(defs *schema.Set) []string {
	parser := func(name string) string {
		return strings.Split(name, "(")[0]
	}

	names := make([]string, defs.Len())
	for _, def := range defs.List() {
		names = append(names, parser(def.(string)))
	}
	return names
}
