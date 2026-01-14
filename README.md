# Terraform Provider for AWS Redshift

This provider allows to manage with Terraform [AWS Redshift](https://aws.amazon.com/redshift/) objects like users, groups, schemas, etc...

It's published on the [OpenTofu Registry](https://search.opentofu.org/provider/dbsystel/redshift/latest).

## Requirements

- [OpenTofu](https://opentofu.org/docs/intro/install/) >= 1.0
- [Go](https://golang.org/doc/install) 1.24 (to build the provider plugin)

## Limitations

### Untested features

Due to limited testing capacities, the following features are not tested/stable yet:

* External Schemas
    * Hive Database
    * RDS Postgres Database
    * RDS MySQL Database
    * Redshift Database
* Temporary Credentials Cluster Identifier
* Temporary Credentials Assume Role
* Datashares

### Using the AWS Redshift Data API

This provider *does* support connecting to the Redshift instance using the AWS Redshift Data API. However, this is not
the default behavior, requires some additional configuration and comes along with some caveats:

* Transactions are not run as real DB-level transactions, but rather as a sequence of individual statements (`BatchExecuteStatement` executes all statements at once and does not support queries while being in transaction mode).
* Due to the unsupported state of transactions, interfering DB interactions might lead to unexpected results.
* In order to
  prevent [errors due to conflicts with concurrent transactions](https://stackoverflow.com/questions/37344942/redshift-could-not-complete-because-of-conflict-with-concurrent-transaction),
  all statements depend on one lock across resources. This may lead to longer execution times, especially when multiple
  resources are created or updated at the same time.

## Building The Provider

```sh
$ git clone git@github.com:dbsystel/terraform-provider-redshift
```

Enter the provider directory and build the provider

```sh
$ cd terraform-provider-redshift
$ make build
```

## Development

If you're new to provider development, a good place to start is the [Extending
Terraform](https://www.terraform.io/docs/extend/index.html) docs.

### Running Tests

Acceptance tests require a running real AWS Redshift cluster.

```sh
TF_ACC=1
TF_ACC_PROVIDER_HOST=registry.opentofu.org
TF_ACC_PROVIDER_NAMESPACE=hashicorp
TF_ACC_TERRAFORM_PATH=<path to tofu binary>
REDSHIFT_DATABASE=redshift
REDSHIFT_ROOT_USERNAME=someotherroot

# redshift external schema data catalog test setup
REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_DATABASE=<data catalog database>
REDSHIFT_EXTERNAL_SCHEMA_DATA_CATALOG_IAM_ROLE_ARNS=<iam role arns comma separated>

# user + password setup
REDSHIFT_HOST=<cluster ip or DNS>
REDSHIFT_USER=root
REDSHIFT_PASSWORD=<password>

# Redshift Data API setup
AWS_REGION=eu-central-1
REDSHIFT_DATA_API_SERVERLESS_WORKGROUP_NAME=some-workgroup
REDSHIFT_TEST_ACC_DEBUG_REDSHIFT_DATA=true
# optional, if the instance is not reachable through TCP/IP using the REDSHIFT_HOST env var
REDSHIFT_TEST_ACC_SKIP_USER_LOGIN=true

make testacc
```

If your cluster is only accessible from within the VPC, you can connect via a socks proxy:

```sh
ALL_PROXY=socks5[h]://[<socks-user>:<socks-password>@]<socks-host>[:<socks-port>]
NO_PROXY=127.0.0.1,192.168.0.0/24,*.example.com,localhost
```

## Documentation

Documentation is generated with
[tfplugindocs](https://github.com/hashicorp/terraform-plugin-docs). Generated
files are in `docs/` and should not be updated manually. They are derived from:

* Schema `Description` fields in the provider Go code.
* [examples/](./examples)
* [templates/](./templates)

Use `go generate` to update generated docs.

## Releasing

Builds and releases are automated with GitHub Actions and [GoReleaser](https://github.com/goreleaser/goreleaser/). To kick off the release, simply create a new tag. The GitHub Release Action will run automatically and as soon as the release is drafted and everything looks good, you can publish it manually.
