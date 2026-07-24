package redshift

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	tfschema "github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/lib/pq"
)

func TestAccRedshiftGrant_SchemaToPublic(t *testing.T) {
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_schema"), "-", "_")
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_user"), "-", "_")
	config := fmt.Sprintf(`
resource "redshift_schema" "test" {
	name = %[1]q
}

resource "redshift_grant" "public" {
	group = "PUBLIC"

	schema = %[1]q
	object_type = "schema"
	privileges  = ["create", "usage"]

	depends_on = [
		redshift_schema.test
	]
}

# Add user with different privileges to see if we do not catch them by accident
resource "redshift_user" "test" {
	name = %[2]q
	password = "Foo123456$"
}
resource "redshift_grant" "user" {
	user = redshift_user.test.name
	schema = %[1]q
	object_type = "schema"
	privileges  = ["usage"]

	depends_on = [
		redshift_schema.test
	]
}
`, schemaName, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", fmt.Sprintf("gn:public_ot:schema_%s", schemaName)),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "schema"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "create"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "usage"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_DatabaseToPublic(t *testing.T) {
	config := `
resource "redshift_grant" "public" {
	group = "public"
	object_type = "database"
	privileges = ["temporary"]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", "gn:public_ot:database"),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "database"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "temp"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_LanguageToPublic(t *testing.T) {
	config := `
resource "redshift_grant" "public" {
	group = "public"
	object_type = "language"
	objects = ["plpgsql"]
	privileges = ["usage"]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", "gn:public_ot:language_plpgsql"),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "language"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "usage"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_TableToPublic(t *testing.T) {
	config := `
resource "redshift_grant" "public" {
	group = "public"

	schema = "pg_catalog"
	object_type = "table"
	objects = ["pg_user_info"]
	privileges = ["select", "insert", "update", "delete", "drop", "references", "alter", "truncate"]
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.public", "id", "gn:public_ot:table_pg_catalog_pg_user_info"),
					resource.TestCheckResourceAttr("redshift_grant.public", "group", "public"),
					resource.TestCheckResourceAttr("redshift_grant.public", "schema", "pg_catalog"),
					resource.TestCheckResourceAttr("redshift_grant.public", "object_type", "table"),
					resource.TestCheckResourceAttr("redshift_grant.public", "objects.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "objects.*", "pg_user_info"),
					resource.TestCheckResourceAttr("redshift_grant.public", "privileges.#", "8"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "insert"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "update"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "delete"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "drop"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "references"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "alter"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.public", "privileges.*", "truncate"),
				),
			},
		},
	})
}

func TestAccRedshiftGrant_Database_TempTemporary(t *testing.T) {
	groupName := generateRandomObjectName("tf_acc_grant_db_temp")
	config1 := fmt.Sprintf(`
resource "redshift_group" "group" {
	name = %[1]q
}
resource "redshift_grant" "grant" {
	group = %[1]q
	object_type = "database"
	privileges = ["temp"]
	depends_on = [redshift_group.group]
}
`, groupName)
	config2 := fmt.Sprintf(`
resource "redshift_group" "group" {
	name = %[1]q
}
resource "redshift_grant" "grant" {
	group = %[1]q
	object_type = "database"
	privileges = ["temporary"]
	depends_on = [redshift_group.group]
}
`, groupName)
	fmt.Println(config2)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config1,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:database", groupName)),
					resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
					resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "database"),
					testCheckTypeSetElems("redshift_grant.grant", "privileges", "temp"),
				),
			},
			{
				Config:             config2,
				Check:              resource.ComposeTestCheckFunc(),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

func TestAccRedshiftGrant_BasicDatabase(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	roleNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role@tf_acc_domain.tld"), "-", "_"),
	}

	for i, groupName := range groupNames {
		userName := userNames[i]
		roleName := roleNames[i]
		config := fmt.Sprintf(`
		resource "redshift_group" "group" {
		  name = %[1]q
		}
		
		resource "redshift_user" "user" {
		  name = %[2]q
		  password = "TestPassword123"
		}

		resource "redshift_role" "role" {
		  name = %[3]q
		}
		
		resource "redshift_grant" "grant" {
		  group = redshift_group.group.name
		  object_type = "database"
		  privileges = ["create", "temporary", "alter"]
		}
		
		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  object_type = "database"
		  privileges = ["temporary"]
		}

		resource "redshift_grant" "grant_role" {
		  role = redshift_role.role.name
		  object_type = "database"
		  privileges = ["create", "temporary"]
		}
		`, groupName, userName, roleName)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:database", groupName)),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "database"),
						testCheckTypeSetElems("redshift_grant.grant", "privileges", "create", "temp", "alter"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:database", userName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "database"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "temp"),

						resource.TestCheckResourceAttr("redshift_grant.grant_role", "id", fmt.Sprintf("rn:%s_ot:database", roleName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "role", roleName),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "object_type", "database"),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "privileges.#", "2"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "create"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "temp"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicSchema(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	roleNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role@tf_acc_domain.tld"), "-", "_"),
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_basic"), "-", "_")

	for i, groupName := range groupNames {
		userName := userNames[i]
		roleName := roleNames[i]
		config := fmt.Sprintf(`
		resource "redshift_user" "user" {
		  name = %[1]q
		}
		
		resource "redshift_group" "group" {
		  name = %[2]q
		}

		resource "redshift_role" "role" {
		  name = %[4]q
		}

		resource "redshift_schema" "schema" {
		  name = %[3]q
		
		  owner = redshift_user.user.name
		}
		
		resource "redshift_grant" "grant" {
		  group = redshift_group.group.name
		  schema = redshift_schema.schema.name
		
		  object_type = "schema"
		  privileges = ["create", "usage", "alter", "drop"]
		}

		resource "redshift_grant" "grant_role" {
		  role = redshift_role.role.name
		  schema = redshift_schema.schema.name
		
		  object_type = "schema"
		  privileges = ["create", "usage", "alter", "drop"]
          depends_on = [redshift_grant.grant]
		}

		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  schema = redshift_schema.schema.name
		  
		  object_type = "schema"
		  privileges = ["create", "usage", "alter", "drop"]
          depends_on = [redshift_grant.grant_role]
		}
		`, userName, groupName, schemaName, roleName)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:schema_%s", groupName, schemaName)),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "schema"),
						testCheckTypeSetElems("redshift_grant.grant", "privileges", "create", "usage", "alter", "drop"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:schema_%s", userName, schemaName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "schema"),
						testCheckTypeSetElems("redshift_grant.grant_user", "privileges", "create", "usage", "alter", "drop"),

						resource.TestCheckResourceAttr("redshift_grant.grant_role", "id", fmt.Sprintf("rn:%s_ot:schema_%s", roleName, schemaName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "role", roleName),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "object_type", "schema"),
						testCheckTypeSetElems("redshift_grant.grant_role", "privileges", "create", "usage", "alter", "drop"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicTable(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	roleNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_role@tf_acc_domain.tld"), "-", "_"),
	}

	for i, groupName := range groupNames {
		userName := userNames[i]
		roleName := roleNames[i]
		config := fmt.Sprintf(`
		resource "redshift_group" "group" {
		  name = %[1]q
		}

		resource "redshift_role" "role" {
		  name = %[3]q
		}
		
		resource "redshift_user" "user" {
		  name = %[2]q
		  password = "TestPassword123"
		}
		
		resource "redshift_grant" "grant" {
		  group = redshift_group.group.name
		  schema = "pg_catalog"
		
		  object_type = "table"
		  objects = ["pg_user_info"]
		  privileges = ["select", "insert", "update", "delete", "drop", "references", "alter", "truncate"]
		}

		resource "redshift_grant" "grant_role" {
		  role = redshift_role.role.name
		  schema = "pg_catalog"
		
		  object_type = "table"
		  objects = ["pg_user_info"]
		  privileges = ["select", "update", "insert", "delete", "drop", "references", "alter", "truncate"]
		}

		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  schema = "pg_catalog"
		
		  object_type = "table"
		  objects = ["pg_user_info"]
		  privileges = ["select", "insert", "update", "delete", "drop", "references", "alter", "truncate"]
		}
		`, groupName, userName, roleName)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:table_pg_catalog_pg_user_info", groupName)),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "schema", "pg_catalog"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "objects.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "objects.*", "pg_user_info"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "alter"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "truncate"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:table_pg_catalog_pg_user_info", userName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "schema", "pg_catalog"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "objects.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "objects.*", "pg_user_info"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "alter"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "truncate"),

						resource.TestCheckResourceAttr("redshift_grant.grant_role", "id", fmt.Sprintf("rn:%s_ot:table_pg_catalog_pg_user_info", roleName)),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "role", roleName),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "schema", "pg_catalog"),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "objects.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "objects.*", "pg_user_info"),
						resource.TestCheckResourceAttr("redshift_grant.grant_role", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "alter"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_role", "privileges.*", "truncate"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicCallables(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	schema := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_basic"), "-", "_")

	for i, groupName := range groupNames {
		userName := userNames[i]
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: testAccRedshiftGrantBasicCallablesConfigUserGroup(userName, groupName, schema),
				},
				{
					PreConfig: func() {
						dbClient := testAccProvider.Meta().(*Client)
						conn, err := dbClient.Connect()
						defer dbClient.Close()
						if err != nil {
							t.Fatalf("couldn't start redshift connection: %s", err)
						}
						err = testAccRedshiftGrantBasicCallablesCreateSchemaAndCallables(t, conn, schema)
						if err != nil {
							t.Fatalf("couldn't setup database: %s", err)
						}
					},
					Config: testAccRedshiftGrantBasicCallablesConfigUserGroupWithGrants(userName, groupName, schema),
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "id", fmt.Sprintf("gn:%s_ot:function_%s_test_call(float,float)", groupName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "object_type", "function"),
						resource.TestCheckResourceAttr("redshift_grant.grant_fun", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_fun", "privileges.*", "execute"),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "id", fmt.Sprintf("gn:%s_ot:procedure_%s_test_call()", groupName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "object_type", "procedure"),
						resource.TestCheckResourceAttr("redshift_grant.grant_proc", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_proc", "privileges.*", "execute"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "id", fmt.Sprintf("un:%s_ot:function_%s_test_call(int,int)_test_call(float,float)", userName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "object_type", "function"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_fun", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user_fun", "privileges.*", "execute"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "id", fmt.Sprintf("un:%s_ot:procedure_%s_test_call()", userName, schema)),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "object_type", "procedure"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user_proc", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user_proc", "privileges.*", "execute"),
					),
				},
				{
					Config:  testAccRedshiftGrantBasicCallablesConfigUserGroupWithGrants(userName, groupName, schema),
					Destroy: true,
				},
				// Creating additional dummy step as TestStep does not have PostConfig
				// property, so clean up cannot be performed in the previous one.
				{
					PreConfig: func() {
						dbClient := testAccProvider.Meta().(*Client)
						conn, err := dbClient.Connect()
						defer dbClient.Close()
						if err != nil {
							t.Errorf("couldn't cleanup resources: %s", err)
						}
						err = testAccRedshiftGrantBasicCallablesDropResources(t, conn, schema)
						if err != nil {
							t.Errorf("couldn't cleanup resources: %s", err)
						}
					},
					Config:   testAccRedshiftGrantBasicCallablesConfigUserGroupWithGrants(userName, groupName, schema),
					PlanOnly: true,
					Destroy:  true,
				},
			},
		})
	}
}

func TestAccRedshiftGrant_BasicLanguage(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	addedLanguage := "sql"
	secondLanguage := "plpgsql"

	for i, groupName := range groupNames {
		userName := userNames[i]
		config := fmt.Sprintf(`
		resource "redshift_user" "user" {
		  name = %[1]q
		}
		
		resource "redshift_group" "group" {
		  name = %[2]q
		}
		
		resource "redshift_grant" "grant" {
		  group  = redshift_group.group.name
		  objects = [%[3]q, %[4]q]
		
		  object_type = "language"
		  privileges = ["usage"]
		}
		
		resource "redshift_grant" "grant_user" {
		  user = redshift_user.user.name
		  objects = [%[3]q, %[4]q]
		
		  object_type = "language"
		  privileges = ["usage"]
		}
		`, userName, groupName, addedLanguage, secondLanguage)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_grant.grant", "id", fmt.Sprintf("gn:%s_ot:language_%s", groupName, testAccRedshiftGrantObjectSetID(addedLanguage, secondLanguage))),
						resource.TestCheckResourceAttr("redshift_grant.grant", "group", groupName),
						resource.TestCheckResourceAttr("redshift_grant.grant", "object_type", "language"),
						resource.TestCheckResourceAttr("redshift_grant.grant", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant", "privileges.*", "usage"),

						resource.TestCheckResourceAttr("redshift_grant.grant_user", "id", fmt.Sprintf("un:%s_ot:language_%s", userName, testAccRedshiftGrantObjectSetID(addedLanguage, secondLanguage))),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "user", userName),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "object_type", "language"),
						resource.TestCheckResourceAttr("redshift_grant.grant_user", "privileges.#", "1"),
						resource.TestCheckTypeSetElemAttr("redshift_grant.grant_user", "privileges.*", "usage"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftGrant_Regression_GH_Issue_24(t *testing.T) {
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_grant"), "-", "_")
	dbName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_db_grant"), "-", "_")

	for _, userName := range userNames {
		config := fmt.Sprintf(`
		resource "redshift_user" "user" {
		name = %[1]q
		}

		# Create a group named the same as user
		resource "redshift_group" "group" {
		name = %[1]q
		}

		# Create a schema and set user as owner
		resource "redshift_schema" "schema" {
		name = %[2]q

		owner = redshift_user.user.name
		}

		# The schema owner user will have all (create, usage) privileges on the schema
		# Set only 'create' privilege to a group with the same name as user. In previous versions this would trigger a permanent diff in plan.
		resource "redshift_grant" "schema" {
			group = redshift_group.group.name
			schema = redshift_schema.schema.name
	
			object_type = "schema"
			privileges = ["create", "usage"]
		}
		`, userName, schemaName, dbName)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      func(s *terraform.State) error { return nil },
			Steps: []resource.TestStep{
				{
					Config: config,
					Check:  resource.ComposeTestCheckFunc(),
				},
				// The 'ExpectNonEmptyPlan: false' option will fail the test if second run on the same config  will show any changes
				{
					Config:             config,
					Check:              resource.ComposeTestCheckFunc(),
					ExpectNonEmptyPlan: false,
				},
			},
		})
	}
}

func TestAccRedshiftGrant_Regression_Issue_43(t *testing.T) {
	// todo: use dynamic names for groups/schemas
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_grant"), "-", "_")

	config := fmt.Sprintf(`
resource "redshift_user" "user" {
  name      = %[1]q
}

resource "redshift_group" "y_schema" {
  name  = "y_schema"
  users = [redshift_user.user.name]
}

resource "redshift_group" "y" {
  name  = "y"
  users = [redshift_user.user.name]
}

resource "redshift_schema" "x" {
  name  = "x"
  owner = redshift_user.user.name
}

resource "redshift_schema" "schema_x" {
  name  = "schema_x"
  owner = redshift_user.user.name
}

resource "redshift_grant" "grants1" {
  group       = redshift_group.y_schema.name
  schema      = redshift_schema.x.name
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "redshift_grant" "grants2" {
  group       = redshift_group.y.name
  schema      = redshift_schema.schema_x.name
  object_type = "schema"
  privileges  = ["USAGE"]
}
`, userName)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check:  testAccRedshiftGrantRegressionIssue43CompareIds("redshift_grant.grants1", "redshift_grant.grants2"),
			},
		},
	})
}

func testAccRedshiftGrantRegressionIssue43CompareIds(addr1 string, addr2 string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs1, ok := s.RootModule().Resources[addr1]
		if !ok {
			return fmt.Errorf("not found: %q", addr1)
		}
		rs2, ok := s.RootModule().Resources[addr2]
		if !ok {
			return fmt.Errorf("not found: %q", addr2)
		}

		if rs1.Primary.ID == rs2.Primary.ID {
			return fmt.Errorf("resources %q and %q have the same ID: %q", addr1, addr2, rs1.Primary.ID)
		}

		return nil
	}
}

func testAccRedshiftGrantObjectSetID(objects ...string) string {
	setItems := make([]interface{}, len(objects))
	for i, object := range objects {
		setItems[i] = object
	}

	set := tfschema.NewSet(tfschema.HashString, setItems)
	orderedObjects := make([]string, 0, len(objects))
	for _, object := range set.List() {
		orderedObjects = append(orderedObjects, object.(string))
	}

	return strings.Join(orderedObjects, "_")
}

func testAccRedshiftGrantBasicCallablesConfigUserGroup(username, group, _ string) string {
	return fmt.Sprintf(`
resource "redshift_user" "user" {
  name = %[1]q
}

resource "redshift_group" "group" {
  name = %[2]q
}
`, username, group)
}

func testAccRedshiftGrantBasicCallablesConfigUserGroupWithGrants(username, group, schema string) string {
	return fmt.Sprintf(`
resource "redshift_user" "user" {
  name = %[1]q
}

resource "redshift_group" "group" {
  name = %[2]q
}

resource "redshift_grant" "grant_fun" {
	schema = %[3]q
  group  = redshift_group.group.name
  objects = ["test_call(float,float)"]

  object_type = "function"
  privileges = ["execute"]
}

resource "redshift_grant" "grant_proc" {
	schema = %[3]q
  group  = redshift_group.group.name
  objects = ["test_call()"]

  object_type = "procedure"
  privileges = ["execute"]
}

resource "redshift_grant" "grant_user_fun" {
	schema = %[3]q
  user = redshift_user.user.name
  objects = ["test_call(float,float)", "test_call(int,int)"]

  object_type = "function"
  privileges = ["execute"]
}

resource "redshift_grant" "grant_user_proc" {
	schema = %[3]q
  user = redshift_user.user.name
  objects = ["test_call()"]

  object_type = "procedure"
  privileges = ["execute"]
}
`, username, group, schema)
}

func testAccRedshiftGrantBasicCallablesCreateSchemaAndCallables(_ *testing.T, db *DBConnection, schema string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s", pq.QuoteIdentifier(schema)))
	if err != nil {
		return fmt.Errorf("couldn't create schema: %s", err)
	}

	function := fmt.Sprintf(`
	create function %s.test_call (a float, b float)
		returns float
	stable
	as $$
		select greatest($1, $2)
	$$ language sql;
`, schema)

	_, err = db.Exec(function)
	if err != nil {
		return fmt.Errorf("couldn't create function: %s", err)
	}

	function2 := fmt.Sprintf(`
	create function %s.test_call (a int, b int)
		returns int
	stable
	as $$
		select greatest($1, $2)
	$$ language sql;
`, schema)

	_, err = db.Exec(function2)
	if err != nil {
		return fmt.Errorf("couldn't create function2: %s", err)
	}

	procedure := fmt.Sprintf(`
	CREATE PROCEDURE %s.test_call() AS $$
		BEGIN
	RAISE NOTICE 'Hello, world!';
		END
	$$ LANGUAGE plpgsql;
	`, schema)

	_, err = db.Exec(procedure)
	if err != nil {
		return fmt.Errorf("couldn't create procedure: %s", err)
	}

	return nil
}

func testAccRedshiftGrantBasicCallablesDropResources(_ *testing.T, db *DBConnection, schema string) error {
	query := fmt.Sprintf("DROP SCHEMA %s CASCADE", pq.QuoteIdentifier(schema))
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("couldn't drop test schema: %s", err)
	}
	return nil
}

// The following tests exercise the read path for a "GRANT ... ON ALL TABLES IN
// SCHEMA" grant (object_type = "table" with an empty objects set). The
// privileges recorded in state are the intersection across every table in the
// schema: a privilege is present only if every table grants it to the grantee.
//
// Each test creates the grantee as a Terraform resource in the first step (which
// also configures the provider), then creates the schema and tables out of band
// from the second step onwards, and drops the schema in CheckDestroy.

// TestAccRedshiftGrant_AllTables_AddRemovePrivileges covers the ordinary
// lifecycle: granting on all tables, adding a privilege, and removing it again.
// Each step's follow-up plan must be empty, and the change must reach every
// table in the schema.
func TestAccRedshiftGrant_AllTables_AddRemovePrivileges(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_alltables"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_alltables"), "-", "_")
	grantID := fmt.Sprintf("un:%s_ot:table_%s", userName, schemaName)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccRedshiftGrantDropSchema(schemaName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantUserConfig(userName),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						return testAccRedshiftGrantCreateSchemaTables(db, schemaName, "table_a", "table_b")
					})
				},
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "id", grantID),
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
				),
			},
			{
				// Add a privilege: it must be granted on every table.
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select", "update"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "2"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "update"),
					testAccCheckUserTablePrivilege(schemaName, "table_a", userName, "update", true),
					testAccCheckUserTablePrivilege(schemaName, "table_b", userName, "update", true),
				),
			},
			{
				// Remove it again: it must be revoked on every table.
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
					testAccCheckUserTablePrivilege(schemaName, "table_b", userName, "select", true),
					testAccCheckUserTablePrivilege(schemaName, "table_b", userName, "update", false),
				),
			},
		},
	})
}

// TestAccRedshiftGrant_AllTables_ExtraPrivilegeIgnored guards the core
// regression: an extra privilege on just one table must not leak into state.
// Because every table still grants the configured privilege, the plan stays
// empty. A PlanOnly step is required — an apply would REVOKE ALL and mask the
// difference before the check runs.
func TestAccRedshiftGrant_AllTables_ExtraPrivilegeIgnored(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_extra"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_extra"), "-", "_")

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccRedshiftGrantDropSchema(schemaName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantUserConfig(userName),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						return testAccRedshiftGrantCreateSchemaTables(db, schemaName, "table_a", "table_b")
					})
				},
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
				),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						_, err := db.Exec(fmt.Sprintf("GRANT UPDATE ON %s.table_b TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(userName)))
						return err
					})
				},
				Config:   testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				PlanOnly: true,
			},
		},
	})
}

// TestAccRedshiftGrant_AllTables_UncoveredTableConverges covers the reported
// symptom: when one table is missing the configured privilege the plan reports
// drift (deterministically), and applying re-grants across every existing table
// so a re-run converges.
func TestAccRedshiftGrant_AllTables_UncoveredTableConverges(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_uncovered"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_uncovered"), "-", "_")

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccRedshiftGrantDropSchema(schemaName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantUserConfig(userName),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						return testAccRedshiftGrantCreateSchemaTables(db, schemaName, "table_a", "table_b")
					})
				},
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					testAccCheckUserTablePrivilege(schemaName, "table_b", userName, "select", true),
				),
			},
			{
				// Revoke the privilege on one table out of band: the plan must now
				// report drift (state reads back as no privileges).
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						_, err := db.Exec(fmt.Sprintf("REVOKE SELECT ON %s.table_b FROM %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(userName)))
						return err
					})
				},
				Config:             testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			},
			{
				// Applying re-grants on every existing table, and the follow-up
				// plan (checked automatically) is empty: the resource converges.
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					testAccCheckUserTablePrivilege(schemaName, "table_b", userName, "select", true),
				),
			},
		},
	})
}

// TestAccRedshiftGrant_AllTables_EmptySchema guards against reporting drift when
// the schema contains no tables. There is nothing to read back, so the
// configured privileges must be left in state and the follow-up plan must be
// empty (an empty set would be permanent, unresolvable drift).
func TestAccRedshiftGrant_AllTables_EmptySchema(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_emptyschema"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_emptyschema"), "-", "_")

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccRedshiftGrantDropSchema(schemaName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantUserConfig(userName),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						return testAccRedshiftGrantCreateSchemaTables(db, schemaName)
					})
				},
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
				),
			},
		},
	})
}

func testAccRedshiftGrantUserConfig(user string) string {
	return fmt.Sprintf(`
resource "redshift_user" "grantee" {
  name = %[1]q
}
`, user)
}

func testAccRedshiftGrantAllTablesConfig(user, schemaName string, privileges ...string) string {
	quoted := make([]string, len(privileges))
	for i, privilege := range privileges {
		quoted[i] = fmt.Sprintf("%q", privilege)
	}
	return testAccRedshiftGrantUserConfig(user) + fmt.Sprintf(`
resource "redshift_grant" "all_tables" {
  user        = redshift_user.grantee.name
  schema      = %[1]q
  object_type = "table"
  objects     = []
  privileges  = [%[2]s]
}
`, schemaName, strings.Join(quoted, ", "))
}

// withAccGrantConn opens a Redshift connection using the configured test
// provider and runs fn against it, failing the test on any error.
func withAccGrantConn(t *testing.T, fn func(db *DBConnection) error) {
	dbClient := testAccProvider.Meta().(*Client)
	conn, err := dbClient.Connect()
	defer dbClient.Close()
	if err != nil {
		t.Fatalf("couldn't start redshift connection: %s", err)
	}
	if err := fn(conn); err != nil {
		t.Fatalf("redshift setup/teardown failed: %s", err)
	}
}

// testAccCheckUserTablePrivilege asserts whether a user holds a specific
// privilege on a specific table, read directly from the catalog.
func testAccCheckUserTablePrivilege(schemaName, table, user, privilege string, want bool) resource.TestCheckFunc {
	return func(*terraform.State) error {
		dbClient := testAccProvider.Meta().(*Client)
		conn, err := dbClient.Connect()
		if err != nil {
			return fmt.Errorf("couldn't connect to redshift: %w", err)
		}
		defer dbClient.Close()

		const query = `
SELECT COALESCE(MAX(CASE WHEN privilege_type = $1 THEN 1 ELSE 0 END), 0)
FROM svv_relation_privileges
WHERE namespace_name = $2 AND relation_name = $3 AND identity_name = $4 AND identity_type = 'user'`
		var granted int
		if err := conn.QueryRow(query, strings.ToUpper(privilege), schemaName, table, user).Scan(&granted); err != nil {
			return fmt.Errorf("couldn't read privileges for %s.%s: %w", schemaName, table, err)
		}
		if (granted == 1) != want {
			return fmt.Errorf("table %s.%s: privilege %q for user %s granted=%v, want %v", schemaName, table, privilege, user, granted == 1, want)
		}
		return nil
	}
}

// testAccRedshiftGrantDropSchema returns a CheckDestroy that removes the test
// schema (and its tables) once the Terraform-managed resources are destroyed.
func testAccRedshiftGrantDropSchema(schemaName string) func(*terraform.State) error {
	return func(*terraform.State) error {
		dbClient := testAccProvider.Meta().(*Client)
		conn, err := dbClient.Connect()
		if err != nil {
			return fmt.Errorf("couldn't connect to redshift: %w", err)
		}
		defer dbClient.Close()
		if _, err := conn.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pq.QuoteIdentifier(schemaName))); err != nil {
			return fmt.Errorf("couldn't drop test schema: %w", err)
		}
		return nil
	}
}

func testAccRedshiftGrantCreateSchemaTables(db *DBConnection, schema string, tables ...string) error {
	if _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s", pq.QuoteIdentifier(schema))); err != nil {
		return fmt.Errorf("couldn't create schema: %w", err)
	}
	for _, table := range tables {
		stmt := fmt.Sprintf("CREATE TABLE %s.%s (id int)", pq.QuoteIdentifier(schema), pq.QuoteIdentifier(table))
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("couldn't create table %s: %w", table, err)
		}
	}
	return nil
}

// TestAccRedshiftGrant_AllTables_MaterializedView guards convergence when the
// schema contains a materialized view. The view's internal storage table
// (mv_tbl__*) is never covered by GRANT ... ON ALL TABLES, so it must be
// excluded from the read; otherwise the intersection would permanently drop the
// granted privilege.
func TestAccRedshiftGrant_AllTables_MaterializedView(t *testing.T) {
	userName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user_matview"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_matview"), "-", "_")

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccRedshiftGrantDropSchema(schemaName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantUserConfig(userName),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						return testAccRedshiftGrantCreateSchemaWithMatview(db, schemaName)
					})
				},
				Config: testAccRedshiftGrantAllTablesConfig(userName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
				),
			},
		},
	})
}

// TestAccRedshiftGrant_AllTables_Group exercises the group query branch: the
// grant converges (including with a materialized view present) and an extra
// privilege on one table is ignored by the intersection.
func TestAccRedshiftGrant_AllTables_Group(t *testing.T) {
	groupName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_alltables"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_group"), "-", "_")
	grantID := fmt.Sprintf("gn:%s_ot:table_%s", groupName, schemaName)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccRedshiftGrantDropSchema(schemaName),
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftGrantGroupConfig(groupName),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						return testAccRedshiftGrantCreateSchemaWithMatview(db, schemaName)
					})
				},
				Config: testAccRedshiftGrantAllTablesGroupConfig(groupName, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "id", grantID),
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
				),
			},
			{
				// Extra privilege on one table, out of band: still ignored.
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						_, err := db.Exec(fmt.Sprintf("GRANT UPDATE ON %s.table_b TO GROUP %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(groupName)))
						return err
					})
				},
				Config:   testAccRedshiftGrantAllTablesGroupConfig(groupName, schemaName, "select"),
				PlanOnly: true,
			},
		},
	})
}

// TestAccRedshiftGrant_AllTables_Public exercises the GRANT TO PUBLIC query
// branch, including convergence with a materialized view present.
func TestAccRedshiftGrant_AllTables_Public(t *testing.T) {
	anchorGroup := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group_anchor"), "-", "_")
	schemaName := strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_schema_public"), "-", "_")
	grantID := fmt.Sprintf("gn:public_ot:table_%s", schemaName)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccRedshiftGrantDropSchema(schemaName),
		Steps: []resource.TestStep{
			{
				// The anchor group only configures the provider so the following
				// PreConfig can open a connection; the grant itself targets PUBLIC.
				Config: testAccRedshiftGrantGroupConfig(anchorGroup),
			},
			{
				PreConfig: func() {
					withAccGrantConn(t, func(db *DBConnection) error {
						return testAccRedshiftGrantCreateSchemaWithMatview(db, schemaName)
					})
				},
				Config: testAccRedshiftGrantAllTablesPublicConfig(anchorGroup, schemaName, "select"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "id", grantID),
					resource.TestCheckResourceAttr("redshift_grant.all_tables", "privileges.#", "1"),
					resource.TestCheckTypeSetElemAttr("redshift_grant.all_tables", "privileges.*", "select"),
				),
			},
		},
	})
}

func testAccRedshiftGrantGroupConfig(group string) string {
	return fmt.Sprintf(`
resource "redshift_group" "grantee" {
  name = %[1]q
}
`, group)
}

func testAccRedshiftGrantAllTablesGroupConfig(group, schemaName string, privileges ...string) string {
	return testAccRedshiftGrantGroupConfig(group) + fmt.Sprintf(`
resource "redshift_grant" "all_tables" {
  group       = redshift_group.grantee.name
  schema      = %[1]q
  object_type = "table"
  objects     = []
  privileges  = [%[2]s]
}
`, schemaName, quotePrivileges(privileges))
}

func testAccRedshiftGrantAllTablesPublicConfig(anchorGroup, schemaName string, privileges ...string) string {
	return testAccRedshiftGrantGroupConfig(anchorGroup) + fmt.Sprintf(`
resource "redshift_grant" "all_tables" {
  group       = "public"
  schema      = %[1]q
  object_type = "table"
  objects     = []
  privileges  = [%[2]s]
}
`, schemaName, quotePrivileges(privileges))
}

func quotePrivileges(privileges []string) string {
	quoted := make([]string, len(privileges))
	for i, privilege := range privileges {
		quoted[i] = fmt.Sprintf("%q", privilege)
	}
	return strings.Join(quoted, ", ")
}

func testAccRedshiftGrantCreateSchemaWithMatview(db *DBConnection, schema string) error {
	if err := testAccRedshiftGrantCreateSchemaTables(db, schema, "table_a", "table_b"); err != nil {
		return err
	}
	stmt := fmt.Sprintf("CREATE MATERIALIZED VIEW %s.mv_a AS SELECT id FROM %s.table_a", pq.QuoteIdentifier(schema), pq.QuoteIdentifier(schema))
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("couldn't create materialized view: %w", err)
	}
	return nil
}
