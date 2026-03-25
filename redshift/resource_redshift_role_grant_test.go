package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftRoleGrant_Basic(t *testing.T) {
	randomObjectName := generateRandomObjectName("acc_test_role_grant")
	roleName := randomObjectName
	userName := fmt.Sprintf("%s_user", randomObjectName)
	secondRoleName := fmt.Sprintf("%s_second_role", randomObjectName)

	configCreate := fmt.Sprintf(`
resource "redshift_role" "role" {
	name = "%s"
}

resource "redshift_user" "user" {
	name = "%s"
}

resource "redshift_role" "second_role" {
	name = "%s"
}

resource "redshift_role_grant" "user" {
	role_name = redshift_role.role.name
	grant_to_type = "USER"
	grant_to_name = redshift_user.user.name
}

resource "redshift_role_grant" "role" {
	role_name = redshift_role.role.name
	grant_to_type = "ROLE"
	grant_to_name = redshift_role.second_role.name
}
`, randomObjectName, userName, secondRoleName)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftRoleGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleGrantExists("user", userName, roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.user", "role_name", roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.user", "grant_to_type", "USER"),
					resource.TestCheckResourceAttr("redshift_role_grant.user", "grant_to_name", userName),

					testAccCheckRedshiftRoleGrantExists("role", secondRoleName, roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.role", "role_name", roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.role", "grant_to_type", "ROLE"),
					resource.TestCheckResourceAttr("redshift_role_grant.role", "grant_to_name", secondRoleName),
				),
			},
		},
	})
}

func TestAccRedshiftRoleGrant_Update(t *testing.T) {
	randomObjectName := generateRandomObjectName("acc_test_role_grant")
	roleName := randomObjectName
	userName := fmt.Sprintf("%s_user", randomObjectName)
	secondRoleName := fmt.Sprintf("%s_second_role", randomObjectName)
	thirdRoleName := fmt.Sprintf("%s_third_role", randomObjectName)

	configBase := fmt.Sprintf(`
resource "redshift_role" "role" {
	name = "%s"
}

resource "redshift_user" "user" {
	name = "%s"
}

resource "redshift_role" "second_role" {
	name = "%s"
}

resource "redshift_role" "third_role" {
	name = "%s"
}`, randomObjectName, userName, secondRoleName, thirdRoleName)

	configCreate := configBase + `
resource "redshift_role_grant" "combined" {
	role_name = redshift_role.role.name
	grant_to_type = "USER"
	grant_to_name = redshift_user.user.name
}`
	configUpdateGrantRole := configBase + `
resource "redshift_role_grant" "combined" {
	role_name = redshift_role.role.name
	grant_to_type = "ROLE"
	grant_to_name = redshift_role.second_role.name
}`
	configUpdateGrantDifferentRole := configBase + `
resource "redshift_role_grant" "combined" {
	role_name = redshift_role.role.name
	grant_to_type = "ROLE"
	grant_to_name = redshift_role.third_role.name
}`

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftRoleGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleGrantExists("user", userName, roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "role_name", roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "grant_to_type", "USER"),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "grant_to_name", userName),
				),
			},
			{
				Config: configUpdateGrantRole,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleGrantExists("role", secondRoleName, roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "role_name", roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "grant_to_type", "ROLE"),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "grant_to_name", secondRoleName),

					testAccCheckRedshiftRoleGrantNotExists("user", userName, roleName),
				),
			},
			{
				Config: configUpdateGrantDifferentRole,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleGrantExists("role", thirdRoleName, roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "role_name", roleName),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "grant_to_type", "ROLE"),
					resource.TestCheckResourceAttr("redshift_role_grant.combined", "grant_to_name", thirdRoleName),

					testAccCheckRedshiftRoleGrantNotExists("role", secondRoleName, roleName),
					testAccCheckRedshiftRoleGrantNotExists("user", userName, roleName),
				),
			},
		},
	})
}

func testAccCheckRedshiftRoleGrantDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_role_grant" {
			continue
		}

		exists, err := checkRoleGrantExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("error checking role grant: %w", err)
		}

		if exists {
			return fmt.Errorf("role grant still exists after destroy")
		}
	}

	return nil
}

func testAccCheckRedshiftRoleGrantExists(grantToType, entityTo, roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkRoleGrantExists(client, generateRoleGrantID(roleName, grantToType, entityTo))
		if err != nil {
			return fmt.Errorf("error checking role grant: %w", err)
		}

		if !exists {
			return fmt.Errorf("role grant not found")
		}

		return nil
	}
}

func testAccCheckRedshiftRoleGrantNotExists(grantToType, entityTo, roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkRoleGrantExists(client, generateRoleGrantID(roleName, grantToType, entityTo))
		if err != nil {
			return fmt.Errorf("error checking role grant: %w", err)
		}

		if exists {
			return fmt.Errorf("role grant still found")
		}

		return nil
	}
}

func checkRoleGrantExists(client *Client, roleGrantId string) (bool, error) {
	role, grantToType, entityTo, err := parseRoleGrantId(roleGrantId)
	if err != nil {
		return false, err
	}
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var resp int
	var query string
	switch grantToType {
	case "USER":
		query = "SELECT 1 FROM svv_user_grants WHERE role_name = $1 and user_name = $2"
	case "ROLE":
		query = "SELECT 1 FROM svv_role_grants WHERE granted_role_name = $1 and role_name = $2"
	default:
		return false, fmt.Errorf("unsupported grant_to_type: %s", grantToType)
	}
	err = db.QueryRow(query, role, entityTo).Scan(&resp)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading info about role: %w", err)
	}

	return true, nil
}

func parseRoleGrantId(roleGrantId string) (roleName, grantToType, grantToName string, err error) {
	// ID format: "role:rolename:type:targetname"
	parts := strings.Split(roleGrantId, ":")
	if len(parts) != 4 {
		return "", "", "", fmt.Errorf("invalid role grant ID format: %s", roleGrantId)
	}
	roleName = parts[1]
	grantToType = strings.ToUpper(parts[2])
	grantToName = parts[3]
	return roleName, grantToType, grantToName, nil
}
