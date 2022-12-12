package main

import (
	"github.com/glympse/terraform-provider-nifi/nifi"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: nifi.Provider,
	})
}
