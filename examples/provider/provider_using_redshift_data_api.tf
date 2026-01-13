provider "redshift" {
  database = var.redshift_database
  data_api {
    workgroup_name = var.redshift_workgroup
    region         = var.aws_region
  }
}