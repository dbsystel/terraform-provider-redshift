package redshift

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-mux/tf5to6server"
	"github.com/hashicorp/terraform-plugin-mux/tf6muxserver"
)

// MuxedProviderServer serves the terraform-plugin-sdk/v2 provider and the
// terraform-plugin-framework provider as a single provider, so that resources and data
// sources can be migrated from the former to the latter one at a time.
func MuxedProviderServer(ctx context.Context) (func() tfprotov6.ProviderServer, error) {
	return muxedProviderServerFor(ctx, Provider().GRPCProvider)
}

// muxedProviderServerFor muxes a specific SDK provider server, which lets the tests reuse
// the provider instance they hold on to.
func muxedProviderServerFor(ctx context.Context, sdkProvider func() tfprotov5.ProviderServer) (func() tfprotov6.ProviderServer, error) {
	upgraded, err := tf5to6server.UpgradeServer(ctx, sdkProvider)
	if err != nil {
		return nil, err
	}

	muxServer, err := tf6muxserver.NewMuxServer(
		ctx,
		func() tfprotov6.ProviderServer { return upgraded },
		providerserver.NewProtocol6(New()),
	)
	if err != nil {
		return nil, err
	}
	return muxServer.ProviderServer, nil
}
