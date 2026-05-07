resource "redshift_assumerole_grant" "grant" {
	iam_role      = "arn:aws:iam::123456789012:role/myrole"
	grant_to_type = "ROLE"
	grant_to_name = "role1"
	privileges    = ["copy"]
}
