package redshift

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccRedshiftGroupMembership_Basic(t *testing.T) {
	groupName := generateRandomObjectName("tf_acc_group_membership")
	userName := generateRandomObjectName("tf_acc_group_membership_user")
	config := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}

resource "redshift_group_membership" "simple" {
  name = redshift_group.simple.name
  users = [redshift_user.simple.name]
}
`, groupName, userName)
	updateConfig := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
	ignore_changes = [
	  users
	]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}
`, groupName, userName)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupMembershipDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "name", groupName),
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "users.0", userName),
					testAccCheckRedshiftGroupMembershipPresence(groupName, userName, true),
				),
			},
			{
				Config: updateConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckRedshiftGroupMembershipPresence(groupName, userName, false),
				),
			},
		},
	})
}

func TestAccRedshiftGroupMembership_UserRemove(t *testing.T) {
	groupName := generateRandomObjectName("tf_acc_group_membership")
	userName := generateRandomObjectName("tf_acc_group_membership_user")
	config1 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}
`, groupName, userName)
	config2 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}

resource "redshift_group_membership" "simple" {
  name = redshift_group.simple.name
  users = [%[2]q]
}
`, groupName, userName)
	config3 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_group_membership" "simple" {
  name = redshift_group.simple.name
  users = [%[2]q]
}
`, groupName, userName)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupMembershipDestroy,
		Steps: []resource.TestStep{
			{
				Config: config1,
			},
			{
				Config: config2,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "name", groupName),
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "users.0", userName),
					testAccCheckRedshiftGroupMembershipPresence(groupName, userName, true),
				),
			},
			{
				Config:      config3,
				ExpectError: regexp.MustCompile("(?s)After applying this test step and performing a `terraform refresh`, the plan was not empty.*redshift_group_membership.simple will be created"),
			},
		},
	})
}

func TestAccRedshiftGroupMembership_Update(t *testing.T) {
	groupName := generateRandomObjectName("tf_acc_group_membership")
	newGroupName := generateRandomObjectName("tf_acc_group_membership")
	userName1 := generateRandomObjectName("tf_acc_group_membership_user")
	userName2 := generateRandomObjectName("tf_acc_group_membership_user")
	userName3 := generateRandomObjectName("tf_acc_group_membership_user")
	config1 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}

resource "redshift_group_membership" "simple" {
  name = redshift_group.simple.name
  users = [redshift_user.simple.name]
}
`, groupName, userName1)
	config2 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}

resource "redshift_user" "also_simple" {
  name = %[3]q
}

resource "redshift_group_membership" "simple" {
  name = redshift_group.simple.name
  users = [redshift_user.simple.name, redshift_user.also_simple.name]
}
`, newGroupName, userName1, userName2)
	config3 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}

resource "redshift_user" "also_simple" {
  name = %[3]q
}

resource "redshift_user" "third_simple" {
  name = %[4]q
}

resource "redshift_group_membership" "simple" {
  name = redshift_group.simple.name
  users = [redshift_user.simple.name, redshift_user.third_simple.name]
}
`, newGroupName, userName1, userName2, userName3)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupMembershipDestroy,
		Steps: []resource.TestStep{
			{
				Config: config1,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "name", groupName),
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "users.0", userName1),
					testAccCheckRedshiftGroupMembershipPresence(groupName, userName1, true),
				),
			},
			{
				Config: config2,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "name", newGroupName),
					resource.TestCheckTypeSetElemAttr("redshift_group_membership.simple", "users.*", userName1),
					resource.TestCheckTypeSetElemAttr("redshift_group_membership.simple", "users.*", userName2),
					testAccCheckRedshiftGroupMembershipPresence(newGroupName, userName1, true),
					testAccCheckRedshiftGroupMembershipPresence(newGroupName, userName2, true),
					testAccCheckRedshiftGroupMembershipPresence(newGroupName, userName3, false),
				),
			},
			{
				Config: config3,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("redshift_group_membership.simple", "name", newGroupName),
					resource.TestCheckTypeSetElemAttr("redshift_group_membership.simple", "users.*", userName1),
					resource.TestCheckTypeSetElemAttr("redshift_group_membership.simple", "users.*", userName3),
					testAccCheckRedshiftGroupMembershipPresence(newGroupName, userName1, true),
					testAccCheckRedshiftGroupMembershipPresence(newGroupName, userName2, false),
					testAccCheckRedshiftGroupMembershipPresence(newGroupName, userName3, true),
				),
			},
		},
	})
}

func TestAccRedshiftGroupMembership_Invalid_EmptyGroupName(t *testing.T) {
	groupName := generateRandomObjectName("tf_acc_group_membership")
	userName := generateRandomObjectName("tf_acc_group_membership_user")
	config1 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}

resource "redshift_group_membership" "simple" {
  name = ""
  users = [redshift_user.simple.name]
}
`, groupName, userName)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupMembershipDestroy,
		Steps: []resource.TestStep{
			{
				Config:      config1,
				ExpectError: regexp.MustCompile("expected length of name to be in the range"),
			},
		},
	})
}

func TestAccRedshiftGroupMembership_Invalid_EmptyUserList(t *testing.T) {
	groupName := generateRandomObjectName("tf_acc_group_membership")
	userName := generateRandomObjectName("tf_acc_group_membership_user")
	config1 := fmt.Sprintf(`
resource "redshift_group" "simple" {
  name = %[1]q

  lifecycle {
    ignore_changes = [
      users
    ]
  }
}

resource "redshift_user" "simple" {
  name = %[2]q
}

resource "redshift_group_membership" "simple" {
  name = redshift_group.simple.name
  users = []
}
`, groupName, userName)
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: testAccProviders,
		CheckDestroy:      testAccCheckRedshiftGroupMembershipDestroy,
		Steps: []resource.TestStep{
			{
				Config:      config1,
				ExpectError: regexp.MustCompile("at least one user must be specified in \"users\""),
			},
		},
	})
}

func testAccCheckRedshiftGroupMembershipPresence(groupName, userName string, shouldBePresent bool) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkGroupMembershipExists(client, groupName, userName)
		if err != nil {
			return fmt.Errorf("error checking user: %w", err)
		}

		if shouldBePresent && !exists {
			return fmt.Errorf("user %s should be present in group %s, but it is not", userName, groupName)
		} else if !shouldBePresent && exists {
			return fmt.Errorf("user %s should not be present in group %s, but it is", userName, groupName)
		}

		return nil
	}
}

func checkGroupMembershipExists(client *Client, groupName string, userNames ...string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var _rez int
	if len(userNames) == 0 {
		return false, nil
	}
	userNamesParam := buildUserStringArray(userNames, true)
	query := fmt.Sprintf(`SELECT 1 FROM pg_group pgg JOIN pg_user pgu ON pgu.usesysid = ANY(pgg.grolist) WHERE pgu.usename IN (%s)  AND pgg.groname = $1`, userNamesParam)
	err = db.QueryRow(query, groupName).Scan(&_rez)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("error reading info about group: %w", err)
	}

	return true, nil
}

func testAccCheckRedshiftGroupMembershipDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "redshift_group_membership" {
			continue
		}

		exists, err := checkGroupMembershipExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("error checking role: %w", err)
		}

		if exists {
			return fmt.Errorf("group still exists after destroy")
		}
	}

	return nil
}

func Test_calculateUserNamesDiff(t *testing.T) {
	type args struct {
		oldUserNames []string
		newUserNames []string
	}
	tests := []struct {
		name                 string
		args                 args
		wantDeletedUserNames []string
		wantAddedUserNames   []string
	}{
		{
			"no changes",
			args{
				[]string{
					"user1",
					"user2",
				},
				[]string{
					"user1",
					"user2",
				},
			},
			[]string{},
			[]string{},
		},
		{
			"add user",
			args{
				[]string{
					"user1",
					"user2",
				},
				[]string{
					"user1",
					"user2",
					"user3",
				},
			},
			[]string{},
			[]string{"user3"},
		},
		{
			"remove user",
			args{
				[]string{
					"user1",
					"user2",
					"user3",
				},
				[]string{
					"user1",
					"user2",
				},
			},
			[]string{"user3"},
			[]string{},
		},
		{
			"add and remove user",
			args{
				[]string{
					"user1",
					"user2",
				},
				[]string{
					"user2",
					"user3",
				},
			},
			[]string{"user1"},
			[]string{"user3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDeletedUserNames, gotAddedUserNames := calculateUserNamesDiff(tt.args.oldUserNames, tt.args.newUserNames)
			if !reflect.DeepEqual(gotDeletedUserNames, tt.wantDeletedUserNames) {
				t.Errorf("calculateUserNamesDiff() gotDeletedUserNames = %v, want %v", gotDeletedUserNames, tt.wantDeletedUserNames)
			}
			if !reflect.DeepEqual(gotAddedUserNames, tt.wantAddedUserNames) {
				t.Errorf("calculateUserNamesDiff() gotAddedUserNames = %v, want %v", gotAddedUserNames, tt.wantAddedUserNames)
			}
		})
	}
}
