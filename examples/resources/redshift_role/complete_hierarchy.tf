# Complete example showing native Redshift role inheritance
# This demonstrates the PROPER way to implement role hierarchy using Redshift native ROLES

# =============================================================================
# 1. Create Roles (hierarchical structure)
# =============================================================================

# Base role: Read-only access
resource "redshift_role" "readonly" {
  name = "readonly_role"
}

# Intermediate role: Can read and write
resource "redshift_role" "readwrite" {
  name = "readwrite_role"
}

# Advanced role: Full access
resource "redshift_role" "admin" {
  name = "admin_role"
}

# =============================================================================
# 2. Grant Permissions to Roles (not to users!)
# =============================================================================

# Grant SELECT to readonly_role
resource "redshift_grant" "readonly_select" {
  role        = redshift_role.readonly.name
  schema      = "analytics"
  object_type = "table"
  privileges  = ["select"]
}

# Grant INSERT/UPDATE to readwrite_role
resource "redshift_grant" "readwrite_dml" {
  role        = redshift_role.readwrite.name
  schema      = "analytics"
  object_type = "table"
  privileges  = ["insert", "update"]
}

# Grant DELETE/DROP to admin_role
resource "redshift_grant" "admin_full" {
  role        = redshift_role.admin.name
  schema      = "analytics"
  object_type = "table"
  privileges  = ["delete", "drop"]
}

# =============================================================================
# 3. Create Role Hierarchy (native inheritance!)
# =============================================================================

# readwrite_role inherits from readonly_role
# This means readwrite_role automatically gets SELECT privilege
resource "redshift_role_grant" "readwrite_inherits_readonly" {
  role_name      = redshift_role.readonly.name
  grant_to_type  = "role"
  grant_to_name  = redshift_role.readwrite.name

  depends_on = [
    redshift_grant.readonly_select
  ]
}

# admin_role inherits from readwrite_role
# This means admin_role gets SELECT, INSERT, UPDATE privileges
resource "redshift_role_grant" "admin_inherits_readwrite" {
  role_name      = redshift_role.readwrite.name
  grant_to_type  = "role"
  grant_to_name  = redshift_role.admin.name

  depends_on = [
    redshift_grant.readwrite_dml,
    redshift_role_grant.readwrite_inherits_readonly
  ]
}

# Final hierarchy:
# readonly_role        → SELECT
# readwrite_role       → SELECT (inherited) + INSERT + UPDATE
# admin_role           → SELECT + INSERT + UPDATE (inherited) + DELETE + DROP

# =============================================================================
# 4. Assign Roles to Users
# =============================================================================

resource "redshift_user" "analyst_junior" {
  name = "junior_analyst"
}

resource "redshift_user" "analyst_senior" {
  name = "senior_analyst"
}

resource "redshift_user" "analyst_lead" {
  name = "lead_analyst"
}

# Grant readonly_role to junior analyst
resource "redshift_role_grant" "junior_gets_readonly" {
  role_name      = redshift_role.readonly.name
  grant_to_type  = "user"
  grant_to_name  = redshift_user.analyst_junior.name
}

# Grant readwrite_role to senior analyst
# They automatically get readonly permissions too!
resource "redshift_role_grant" "senior_gets_readwrite" {
  role_name      = redshift_role.readwrite.name
  grant_to_type  = "user"
  grant_to_name  = redshift_user.analyst_senior.name
}

# Grant admin_role to lead analyst
# They automatically get all permissions!
resource "redshift_role_grant" "lead_gets_admin" {
  role_name      = redshift_role.admin.name
  grant_to_type  = "user"
  grant_to_name  = redshift_user.analyst_lead.name
}

# =============================================================================
# 5. Also works with Groups!
# =============================================================================

resource "redshift_group" "analysts_group" {
  name = "all_analysts"
}

# Grant readonly role to the entire group
resource "redshift_role_grant" "group_gets_readonly" {
  role_name      = redshift_role.readonly.name
  grant_to_type  = "group"
  grant_to_name  = redshift_group.analysts_group.name
}

# =============================================================================
# Result Summary:
# =============================================================================
# 
# junior_analyst   → readonly_role        → SELECT
# senior_analyst   → readwrite_role       → SELECT + INSERT + UPDATE
# lead_analyst     → admin_role           → SELECT + INSERT + UPDATE + DELETE + DROP
# all_analysts     → readonly_role        → SELECT
#
# NO DUPLICATION! Each permission is defined once at the role level.
# Changes to readonly_role automatically propagate to readwrite_role and admin_role.
