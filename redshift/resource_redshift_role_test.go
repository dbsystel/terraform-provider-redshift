package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"text/template"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	testAccRedshiftRoleConfigTempl, _ = template.New("test_acc_role_config").Parse(`
resource "redshift_role" "readonly" {
  name = "{{ . }}_readonly"
}

resource "redshift_role" "readwrite" {
  name = "{{ . }}_readwrite"
}

resource "redshift_role" "admin" {
  name = "{{ . }}_admin"
}
`)
)

func TestAccRedshiftRole_Basic(t *testing.T) {
	randomObjectName := generateRandomObjectName("acc_test")
	builder := &strings.Builder{}
	_ = testAccRedshiftRoleConfigTempl.Execute(builder, randomObjectName)

	testAccRedshiftRoleConfig := builder.String()

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccRedshiftRoleConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists(fmt.Sprintf("%s_readonly", randomObjectName)),
					resource.TestCheckResourceAttr("redshift_role.readonly", "name", fmt.Sprintf("%s_readonly", randomObjectName)),

					testAccCheckRedshiftRoleExists(fmt.Sprintf("%s_readwrite", randomObjectName)),
					resource.TestCheckResourceAttr("redshift_role.readwrite", "name", fmt.Sprintf("%s_readwrite", randomObjectName)),

					testAccCheckRedshiftRoleExists(fmt.Sprintf("%s_admin", randomObjectName)),
					resource.TestCheckResourceAttr("redshift_role.admin", "name", fmt.Sprintf("%s_admin", randomObjectName)),
				),
			},
		},
	})
}

func TestAccRedshiftRole_Update(t *testing.T) {
	roleName := generateRandomObjectName("acc_test_u")
	roleNameUpdate := fmt.Sprintf("%s_updated", roleName)

	configCreate := fmt.Sprintf(`
resource "redshift_role" "role" {
  name = "%s"
}`, roleName)

	configUpdate := fmt.Sprintf(`
resource "redshift_role" "role" {
name = "%s"
}`, roleNameUpdate)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists(roleName),
					resource.TestCheckResourceAttr("redshift_role.role", "name", roleName),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists(roleNameUpdate),
					resource.TestCheckResourceAttr("redshift_role.role", "name", roleNameUpdate),
				),
			},
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftRoleExists(roleName),
					resource.TestCheckResourceAttr("redshift_role.role", "name", roleName),
				),
			},
		},
	})
}

func testAccCheckRedshiftRoleDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_role" {
			continue
		}

		exists, err := checkRoleExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("error checking role: %w", err)
		}

		if exists {
			return fmt.Errorf("role still exists after destroy")
		}
	}

	return nil
}

func testAccCheckRedshiftRoleExists(roleName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkRoleExists(client, roleName)
		if err != nil {
			return fmt.Errorf("error checking role: %w", err)
		}

		if !exists {
			return fmt.Errorf("role not found")
		}

		return nil
	}
}

func checkRoleExists(client *Client, role string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var resp int
	err = db.QueryRow("SELECT 1 FROM SVV_ROLES WHERE role_name = $1", role).Scan(&resp)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading info about role: %w", err)
	}

	return true, nil
}
