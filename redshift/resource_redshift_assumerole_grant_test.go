package redshift

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftAssumeRoleGrant_BasicUser(t *testing.T) {
	iamRoleArn := os.Getenv("REDSHIFT_IAM_ROLE_ARN")
	if iamRoleArn == "" {
		t.Skip("REDSHIFT_IAM_ROLE_ARN not set, skipping acceptance test")
	}
	userName := generateRandomObjectName("acc_test_assume_grant")

	config := fmt.Sprintf(`
resource "redshift_user" "user" {
	name = %[1]q
}

resource "redshift_assumerole_grant" "grant" {
	iam_role      = %[2]q
	grant_to_type = "USER"
	grant_to_name = redshift_user.user.name
	privileges    = ["copy", "unload"]
}
`, userName, iamRoleArn)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_assumerole_grant.grant", "iam_role", iamRoleArn),
					resource.TestCheckResourceAttr("redshift_assumerole_grant.grant", "grant_to_type", "USER"),
					resource.TestCheckResourceAttr("redshift_assumerole_grant.grant", "grant_to_name", userName),
					testCheckTypeSetElems("redshift_assumerole_grant.grant", "privileges", "copy", "unload"),
				),
			},
		},
	})
}

func TestAccRedshiftAssumeRoleGrant_BasicRole(t *testing.T) {
	iamRoleArn := os.Getenv("REDSHIFT_IAM_ROLE_ARN")
	if iamRoleArn == "" {
		t.Skip("REDSHIFT_IAM_ROLE_ARN not set, skipping acceptance test")
	}
	roleName := generateRandomObjectName("acc_test_assume_grant_role")

	config := fmt.Sprintf(`
resource "redshift_role" "role" {
	name = %[1]q
}

resource "redshift_assumerole_grant" "grant" {
	iam_role      = %[2]q
	grant_to_type = "ROLE"
	grant_to_name = redshift_role.role.name
	privileges    = ["copy"]
}
`, roleName, iamRoleArn)

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      func(s *terraform.State) error { return nil },
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_assumerole_grant.grant", "iam_role", iamRoleArn),
					resource.TestCheckResourceAttr("redshift_assumerole_grant.grant", "grant_to_type", "ROLE"),
					resource.TestCheckResourceAttr("redshift_assumerole_grant.grant", "grant_to_name", roleName),
					testCheckTypeSetElems("redshift_assumerole_grant.grant", "privileges", "copy"),
				),
			},
		},
	})
}
