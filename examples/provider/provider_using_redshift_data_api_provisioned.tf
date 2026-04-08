provider "redshift" {
  database = var.redshift_database
  data_api {
    cluster_identifier = var.redshift_cluster_identifier
    username           = var.redshift_username
    region             = var.aws_region
  }
}
