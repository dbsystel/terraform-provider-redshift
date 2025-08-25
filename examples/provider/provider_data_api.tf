provider "redshift" {
  database = "exampledb"
  data_api {
    workgroup_name = "example-workgroup"
    region         = "us-west-2"
  }
}
