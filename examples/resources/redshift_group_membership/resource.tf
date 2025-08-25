resource "redshift_group_membership" "simple" {
  name = "some_group_name"
  users = ["user1", "user2"]
}
