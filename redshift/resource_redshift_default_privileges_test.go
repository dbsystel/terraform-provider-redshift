package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftDefaultPrivileges_Basic(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	rootUsername := getRootUsername()

	for i, groupName := range groupNames {
		userName := userNames[i]
		config := fmt.Sprintf(`
		resource "redshift_group" "group" {
		  name = %[1]q
		}
		
		resource "redshift_user" "user" {
		  name = %[2]q
		  password = "TestPassword123"
		}
		
		resource "redshift_default_privileges" "group" {
		  group = redshift_group.group.name
		  owner = %[3]q
		  object_type = "table"
		  privileges = ["select", "update", "insert", "delete", "drop", "references", "rule", "trigger"]
		}
		
		resource "redshift_default_privileges" "user" {
		  user = redshift_user.user.name
		  owner = %[3]q
		  object_type = "table"
		  privileges = ["select", "update", "insert", "delete", "drop", "references", "rule", "trigger"]
		}
		`, groupName, userName, rootUsername)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      testAccCheckDefaultPrivilegesDestory(defaultPrivilegesAllSchemasID, 100, "r", groupName),
			Steps: []resource.TestStep{
				{
					Config: config,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "id", fmt.Sprintf("gn:%s_noschema_on:root_ot:table", groupName)),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "group", groupName),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "rule"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "trigger"),

						resource.TestCheckResourceAttr("redshift_default_privileges.user", "id", fmt.Sprintf("un:%s_noschema_on:root_ot:table", userName)),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "user", userName),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "rule"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "trigger"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftDefaultPrivileges_UpdateToRevoke(t *testing.T) {
	groupNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_group@tf_acc_domain.tld"), "-", "_"),
	}
	userNames := []string{
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user"), "-", "_"),
		strings.ReplaceAll(acctest.RandomWithPrefix("tf_acc_user@tf_acc_domain.tld"), "-", "_"),
	}
	rootUsername := getRootUsername()

	for i, groupName := range groupNames {
		userName := userNames[i]
		configInitial := fmt.Sprintf(`
		resource "redshift_group" "group" {
		  name = %[1]q
		}
		
		resource "redshift_user" "user" {
		  name = %[2]q
		  password = "TestPassword123"
		}
		
		resource "redshift_default_privileges" "group" {
		  group = redshift_group.group.name
		  owner = %[3]q
		  object_type = "table"
		  privileges = ["select", "update", "insert", "delete", "drop", "references", "rule", "trigger"]
		}
		
		resource "redshift_default_privileges" "user" {
		  user = redshift_user.user.name
		  owner = %[3]q
		  object_type = "table"
		  privileges = ["select", "update", "insert", "delete", "drop", "references", "rule", "trigger"]
		}
		`, groupName, userName, rootUsername)

		configUpdated := fmt.Sprintf(`
		resource "redshift_group" "group" {
		  name = %[1]q
		}
		
		resource "redshift_user" "user" {
		  name = %[2]q
		  password = "TestPassword123"
		}
		
		resource "redshift_default_privileges" "group" {
		  group = redshift_group.group.name
		  owner = %[3]q
		  object_type = "table"
		  privileges = []
		}
		
		resource "redshift_default_privileges" "user" {
		  user = redshift_user.user.name
		  owner = %[3]q
		  object_type = "table"
		  privileges = []
		}
		`, groupName, userName, rootUsername)
		resource.Test(t, resource.TestCase{
			PreCheck:          func() { testAccPreCheck(t) },
			ProviderFactories: testAccProviders,
			CheckDestroy:      testAccCheckDefaultPrivilegesDestory(defaultPrivilegesAllSchemasID, 100, "r", groupName),
			Steps: []resource.TestStep{
				{
					Config: configInitial,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "id", fmt.Sprintf("gn:%s_noschema_on:root_ot:table", groupName)),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "group", groupName),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "rule"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.group", "privileges.*", "trigger"),

						resource.TestCheckResourceAttr("redshift_default_privileges.user", "id", fmt.Sprintf("un:%s_noschema_on:root_ot:table", userName)),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "user", userName),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "privileges.#", "8"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "select"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "update"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "insert"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "delete"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "drop"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "references"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "rule"),
						resource.TestCheckTypeSetElemAttr("redshift_default_privileges.user", "privileges.*", "trigger"),
					),
				},
				{
					Config: configUpdated,
					Check: resource.ComposeTestCheckFunc(
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "id", fmt.Sprintf("gn:%s_noschema_on:root_ot:table", groupName)),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "group", groupName),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_default_privileges.group", "privileges.#", "0"),

						resource.TestCheckResourceAttr("redshift_default_privileges.user", "id", fmt.Sprintf("un:%s_noschema_on:root_ot:table", userName)),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "user", userName),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "object_type", "table"),
						resource.TestCheckResourceAttr("redshift_default_privileges.user", "privileges.#", "0"),
					),
				},
			},
		})
	}
}

func TestAccRedshiftDefaultPrivileges_BothUserGroupError(t *testing.T) {
	rootUsername := getRootUsername()
	config := fmt.Sprintf(`
resource "redshift_default_privileges" "both" {
  user = "test_user"
  group = "test_group"

  owner = %[1]q
  object_type = "table"
  privileges = []
}
`, rootUsername)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config:      config,
				ExpectError: regexp.MustCompile("only one of `group,user` can be specified"),
			},
		},
	})
}

func TestAccRedshiftDefaultPrivileges_NoUserGroupError(t *testing.T) {
	rootUsername := getRootUsername()
	config := fmt.Sprintf(`
resource "redshift_default_privileges" "none" {
  owner = %[1]q
  object_type = "table"
  privileges = []
}
`, rootUsername)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config:      config,
				ExpectError: regexp.MustCompile("one of `group,user` must be specified"),
			},
		},
	})
}

func testAccCheckDefaultPrivilegesDestory(schemaID, ownerID int, objectType, groupName string) func(*terraform.State) error {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		for _, rs := range s.RootModule().Resources {
			if rs.Type != "redshift_default_privileges" {
				continue
			}

			exists, err := checkDefACLExists(client, schemaID, ownerID, objectType, groupName)

			if err != nil {
				return fmt.Errorf("error checking role: %w", err)
			}

			if exists {
				return fmt.Errorf("user still exists after destroy")
			}
		}

		return nil
	}
}

func checkDefACLExists(client *Client, schemaID, ownerID int, objectType, groupName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}

	var _rez int
	err = db.QueryRow(
		fmt.Sprintf("SELECT 1 from pg_default_acl WHERE defaclobjtype=$1 AND defaclnamespace=$2 AND defacluser=$3 AND array_to_string(defaclacl, '|') LIKE '%%%s=%%'", groupName),
		objectType,
		schemaID,
		ownerID,
	).Scan(&_rez)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading info about default ACL: %w", err)
	}

	return true, nil
}

func getRootUsername() string {
	envRootUsername := os.Getenv("REDSHIFT_ROOT_USERNAME")
	if envRootUsername == "" {
		return "root"
	}
	return envRootUsername
}
