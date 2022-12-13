package provider

import (
	"context"
	"fmt"
	"log"

	nifi "github.com/glympse/terraform-provider-nifi/nifi"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ResourceProcessGroup() *schema.Resource {
	return &schema.Resource{
		CreateContext: ResourceProcessGroupCreate,
		ReadContext:   ResourceProcessGroupRead,
		UpdateContext: ResourceProcessGroupUpdate,
		DeleteContext: ResourceProcessGroupDelete,
		Schema: map[string]*schema.Schema{
			"parent_group_id": SchemaParentGroupId(),
			"revision":        SchemaRevision(),
			"component": {
				Type:     schema.TypeList,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"parent_group_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"position": SchemaPosition(),
					},
				},
			},
		},
	}
}

func ResourceProcessGroupCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	processGroup := nifi.ProcessGroup{}
	processGroup.Revision.Version = 0

	err := ProcessGroupFromSchema(d, &processGroup)
	if err != nil {
		return diag.Errorf("Failed to parse Process Group schema")
	}
	parentGroupId := processGroup.Component.ParentGroupId

	client := meta.(*nifi.Client)
	err = client.CreateProcessGroup(&processGroup)
	if err != nil {
		return diag.Errorf("Failed to create Process Group")
	}

	d.SetId(processGroup.Component.Id)
	d.Set("parent_group_id", parentGroupId)

	return ResourceProcessGroupRead(ctx, d, meta)
}

func ResourceProcessGroupRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	processGroupId := d.Id()

	client := meta.(*nifi.Client)
	processGroup, err := client.GetProcessGroup(processGroupId)
	if err != nil {
		return diag.Errorf("error retrieving Process Group: %s", processGroupId)
	}

	err = ProcessGroupToSchema(d, processGroup)
	if err != nil {
		return diag.Errorf("Failed to serialize Process Group: %s", processGroupId)
	}

	return nil
}

func ResourceProcessGroupUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	processGroupId := d.Id()

	client := meta.(*nifi.Client)
	processGroup, err := client.GetProcessGroup(processGroupId)
	if err != nil {
		if "not_found" == err.Error() {
			d.SetId("")
			return nil
		} else {
			return diag.Errorf("error retrieving Process Group: %s", processGroupId)
		}
	}

	err = ProcessGroupFromSchema(d, processGroup)
	if err != nil {
		return diag.Errorf("Failed to parse Process Group schema: %s", processGroupId)
	}

	err = client.UpdateProcessGroup(processGroup)
	if err != nil {
		return diag.Errorf("Failed to update Process Group: %s", processGroupId)
	}

	return ResourceProcessGroupRead(ctx, d, meta)
}

func ResourceProcessGroupDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	processGroupId := d.Id()
	log.Printf("[INFO] Deleting Process Group: %s", processGroupId)

	client := meta.(*nifi.Client)
	processGroup, err := client.GetProcessGroup(processGroupId)
	if nil != err {
		if "not_found" == err.Error() {
			d.SetId("")
			return nil
		} else {
			return diag.Errorf("error retrieving Process Group: %s", processGroupId)
		}
	}

	err = client.DeleteProcessGroup(processGroup)
	if err != nil {
		return diag.Errorf("error deleting Process Group: %s", processGroupId)
	}

	d.SetId("")
	return nil
}

func ResourceProcessGroupExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	processGroupId := d.Id()

	client := meta.(*nifi.Client)
	_, err := client.GetProcessGroup(processGroupId)
	if nil != err {
		if err.Error() == "not_found" {
			log.Printf("[INFO] Process Group %s no longer exists, removing from state...", processGroupId)
			d.SetId("")
			return false, nil
		} else {
			return false, fmt.Errorf("error testing existence of Process Group: %s", processGroupId)
		}
	}

	return true, nil
}

// Schema Helpers

func ProcessGroupFromSchema(d *schema.ResourceData, processGroup *nifi.ProcessGroup) error {
	v := d.Get("component").([]interface{})
	if len(v) != 1 {
		return fmt.Errorf("exactly one component is required")
	}
	component := v[0].(map[string]interface{})

	parentGroupId := component["parent_group_id"].(string)
	processGroup.Component.ParentGroupId = parentGroupId
	processGroup.Component.Name = component["name"].(string)

	v = component["position"].([]interface{})
	if len(v) != 1 {
		return fmt.Errorf("exactly one component.position is required")
	}
	position := v[0].(map[string]interface{})
	processGroup.Component.Position.X = position["x"].(float64)
	processGroup.Component.Position.Y = position["y"].(float64)

	return nil
}

func ProcessGroupToSchema(d *schema.ResourceData, processGroup *nifi.ProcessGroup) error {
	revision := []map[string]interface{}{{
		"version": processGroup.Revision.Version,
	}}
	d.Set("revision", revision)

	component := []map[string]interface{}{{
		"parent_group_id": d.Get("parent_group_id").(string),
		"name":            processGroup.Component.Name,
		"position": []map[string]interface{}{{
			"x": processGroup.Component.Position.X,
			"y": processGroup.Component.Position.Y,
		}},
	}}
	d.Set("component", component)

	return nil
}
