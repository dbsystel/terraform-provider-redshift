package main

import (
	"context"
	"log"

	"github.com/dbsystel/terraform-provider-redshift/redshift"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

//go:generate go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs generate --providers-schema schema.json

func main() {
	err := providerserver.Serve(context.Background(), redshift.New, providerserver.ServeOpts{
		Address: "registry.opentofu.org/dbsystel/redshift",
	})
	if err != nil {
		log.Fatal(err)
	}
}
