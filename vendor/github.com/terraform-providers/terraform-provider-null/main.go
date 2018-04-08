package main

import (
	"github.com/hashicorp/terraform/plugin"
	"github.com/mproffitt/terraform-provider-null/null"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: null.Provider})
}
