package main

import (
	"github.com/hashicorp/terraform/plugin"
	"github.com/mproffitt/terraform-provider-random/random"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: random.Provider})
}
