provider "redshift" {
  host     = var.redshift_host
  username = var.redshift_user
  password = var.redshift_password

  # Report an error quickly when the cluster is unreachable, rather than waiting
  # the default 180 seconds.
  connect_timeout = 5

  session_parameters = {
    # Cancel any statement running for longer than five minutes. Redshift expresses
    # this in milliseconds.
    statement_timeout = "300000"

    # Run provider statements in the reserved superuser queue so that they are not
    # held up behind operational queries.
    query_group = "superuser"
  }
}
