package main

import (
	"context"
	"log"

	"github.com/dbsystel/terraform-provider-redshift/redshift"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6/tf6server"
)

//go:generate go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs generate --providers-schema schema.json

func main() {
	ctx := context.Background()

	provider, err := redshift.MuxedProviderServer(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if err := tf6server.Serve("registry.opentofu.org/dbsystel/redshift", provider); err != nil {
		log.Fatal(err)
	}
}
