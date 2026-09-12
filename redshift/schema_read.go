package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

// externalSchemaInfo is what the cluster reports about an external schema, in a form
// both the redshift_schema resource and the redshift_schema data source can shape into
// their own models.
type externalSchemaInfo struct {
	SourceType      string
	DatabaseName    string
	IamRoleArns     []string
	CatalogRoleArns []string
	Region          string
	SourceSchema    string
	Hostname        string
	Port            int64
	SecretArn       string
}

// readSchemaBasics returns the name, owner and type of the schema with the given oid.
// It reports sql.ErrNoRows when the schema is gone.
func readSchemaBasics(db *DBConnection, schemaID string) (name, owner, schemaType string, err error) {
	err = db.QueryRow(`
			SELECT
				TRIM(svv_all_schemas.schema_name),
				TRIM(pg_user_info.usename),
				TRIM(svv_all_schemas.schema_type)
			FROM svv_all_schemas
			INNER JOIN pg_namespace ON (svv_all_schemas.database_name = $1 AND svv_all_schemas.schema_name = pg_namespace.nspname)
	LEFT JOIN pg_user_info
		ON (svv_all_schemas.database_name = $1 AND pg_user_info.usesysid = svv_all_schemas.schema_owner)
	WHERE svv_all_schemas.database_name = $1
	AND pg_namespace.oid = $2`, db.client.config.Database, schemaID).Scan(&name, &owner, &schemaType)
	return name, owner, schemaType, err
}

// readSchemaIDByName returns the oid, owner and type of the schema with the given name.
func readSchemaIDByName(db *DBConnection, schemaName string) (id, owner, schemaType string, err error) {
	err = db.QueryRow(`
			SELECT
				pg_namespace.oid,
				TRIM(pg_user_info.usename),
				TRIM(svv_all_schemas.schema_type)
			FROM svv_all_schemas
			INNER JOIN pg_namespace ON (svv_all_schemas.database_name = $1 AND svv_all_schemas.schema_name = pg_namespace.nspname)
	LEFT JOIN pg_user_info
		ON (svv_all_schemas.database_name = $1 AND pg_user_info.usesysid = svv_all_schemas.schema_owner)
	WHERE svv_all_schemas.database_name = $1
	AND svv_all_schemas.schema_name = $2`, db.client.config.Database, schemaName).Scan(&id, &owner, &schemaType)
	return id, owner, schemaType, err
}

// readSchemaQuota returns the quota of a local schema, in MB.
//
// svv_redshift_schema_quota reports the quota in MB on every cluster type, so one
// query serves Serverless, Multi-AZ and single-AZ provisioned alike. The view also
// lists schemas of datashare-visible databases, and its shared-schema rows join the
// quota on schema_id alone, which is unique only within a database -- so constraining
// database_name is what keeps this lookup unambiguous.
//
// Every schema of the connected database is listed, carrying a NULL quota when none
// is set. A missing row therefore means the lookup failed rather than that the schema
// is unlimited, and reporting that as quota 0 would produce a phantom diff that
// Terraform would "correct" by clearing a live quota.
func readSchemaQuota(db *DBConnection, schemaName string) (int64, error) {
	var schemaQuota sql.NullInt64
	err := db.QueryRow(`
		SELECT quota
		FROM svv_redshift_schema_quota
		WHERE database_name = $1
		  AND schema_name = $2
	`, db.client.config.Database, schemaName).Scan(&schemaQuota)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("schema %q was not found in svv_redshift_schema_quota for database %q: cannot determine its quota", schemaName, db.client.config.Database)
	case err != nil:
		return 0, err
	}

	// A NULL quota means the schema is unlimited, which the resource models as 0.
	if !schemaQuota.Valid {
		return 0, nil
	}
	return schemaQuota.Int64, nil
}

func readExternalSchema(db *DBConnection, schemaID string) (*externalSchemaInfo, error) {
	var sourceType, sourceDbName, iamRole, catalogRole, region, sourceSchema, hostName, port, secretArn string
	err := db.QueryRow(`
	SELECT
		CASE
			WHEN eskind = 1 THEN 'data_catalog_source'
			WHEN eskind = 2 THEN 'hive_metastore_source'
			WHEN eskind = 3 THEN 'rds_postgres_source'
			WHEN eskind = 4 THEN 'redshift_source'
			WHEN eskind = 7 THEN 'rds_mysql_source'
			ELSE 'unknown'
		END,
		TRIM(databasename),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'IAM_ROLE') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'CATALOG_ROLE') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'REGION') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'SCHEMA') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'URI') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'PORT') END, ''),
		COALESCE(CASE WHEN is_valid_json(esoptions) THEN json_extract_path_text(esoptions, 'SECRET_ARN') END, '')
	FROM
	  svv_external_schemas
	WHERE
	  esoid = $1`, schemaID).Scan(&sourceType, &sourceDbName, &iamRole, &catalogRole, &region, &sourceSchema, &hostName, &port, &secretArn)
	if err != nil {
		return nil, err
	}

	info := &externalSchemaInfo{
		SourceType:   sourceType,
		DatabaseName: sourceDbName,
		Region:       region,
		SourceSchema: sourceSchema,
		Hostname:     hostName,
		SecretArn:    secretArn,
	}

	switch sourceType {
	case "data_catalog_source", "hive_metastore_source", "rds_postgres_source", "rds_mysql_source", "redshift_source":
	default:
		return nil, fmt.Errorf(`unsupported source database type: %q`, sourceType)
	}

	if info.IamRoleArns, err = splitCsvAndTrim(iamRole); err != nil {
		return nil, fmt.Errorf("error parsing iam_role_arns: %w", err)
	}
	if info.CatalogRoleArns, err = splitCsvAndTrim(catalogRole); err != nil {
		return nil, fmt.Errorf("error parsing catalog_role_arns: %w", err)
	}
	if port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("%s port was not an integer", sourceType)
		}
		info.Port = int64(portNumber)
	}

	return info, nil
}
