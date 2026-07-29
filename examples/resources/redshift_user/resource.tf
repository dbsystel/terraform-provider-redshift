resource "redshift_user" "user" {
  name      = "UserName"
  password  = "secret password"
  superuser = true
}

resource "redshift_user" "user_with_unrestricted_syslog" {
  name          = "user_syslog"
  syslog_access = "UNRESTRICTED"
}

resource "redshift_user" "reporting" {
  name = "reporting_service"

  # Per-user session defaults, applied via ALTER USER ... SET.
  session_parameters = {
    query_group = "reporting"
    search_path = "$user, public"
  }
}
