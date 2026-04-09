resource "redshift_assumerole_grant" "grant" {
	role_name     = "arn:aws:iam::123456789012:role/myrole"
	grant_to_type = "ROLE"
	grant_to_name = "role1"
	privileges    = ["copy"]
}
