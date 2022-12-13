package nifi

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Provider returns a terraform.ResourceProvider.
func Provider() *schema.Provider {
	p := &schema.Provider{
		Schema: map[string]*schema.Schema{
			"host": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("NIFI_HOST", nil),
			},
			"http_scheme": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("NIFI_HTTP_SCHEME", "https"),
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("NIFI_USERNAME", ""),
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("NIFI_USERNAME", ""),
			},

			"api_path": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("NIFI_API_PATH", "nifi-api"),
			},
			"admin_cert": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("NIFI_ADMIN_CERT", ""),
			},
			"admin_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("NIFI_ADMIN_KEY", ""),
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"nifi_process_group":        ResourceProcessGroup(),
			"nifi_processor":            ResourceProcessor(),
			"nifi_connection":           ResourceConnection(),
			"nifi_controller_service":   ResourceControllerService(),
			"nifi_user":                 ResourceUser(),
			"nifi_group":                ResourceGroup(),
			"nifi_port":                 ResourcePort(),
			"nifi_remote_process_group": ResourceRemoteProcessGroup(),
			"nifi_funnel":               ResourceFunnel(),
			"nifi_reporting_task":       ResourceReportingTask(),
		},
	}
	p.ConfigureContextFunc = func(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
		return providerConfigure(d, p.TerraformVersion)
	}
	return p
}

func providerConfigure(d *schema.ResourceData, terraformVersion string) (interface{}, diag.Diagnostics) {
	config := Config{
		Host:          d.Get("host").(string),
		HttpScheme:    d.Get("http_scheme").(string),
		ApiPath:       d.Get("api_path").(string),
		AdminCertPath: d.Get("admin_cert").(string),
		AdminKeyPath:  d.Get("admin_key").(string),
	}
	client, err := NewClient(config)
	if err != nil {
		return nil, diag.FromErr(err)
	}
	return client, nil
}
