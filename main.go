package main

import (
	"flag"

	"github.com/glympse/terraform-provider-nifi/provider"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	debugFlag := flag.Bool("debug", false, "Start provider in stand-alone debug mode.")
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: provider.Provider,
		Debug:        *debugFlag,
	})

}
