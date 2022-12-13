package provider

import (
	"fmt"
	"log"

	nifi "github.com/glympse/terraform-provider-nifi/nifi"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ResourceGroup() *schema.Resource {
	return &schema.Resource{
		Create: ResourceGroupCreate,
		Read:   ResourceGroupRead,
		Update: ResourceGroupUpdate,
		Delete: ResourceGroupDelete,
		Exists: ResourceGroupExists,
		Importer: &schema.ResourceImporter{
			State: func(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				return []*schema.ResourceData{d}, nil
			},
		},

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
						"identity": {
							Type:     schema.TypeString,
							Required: true,
						},
						"position": SchemaPosition(),
						"users": &schema.Schema{
							Type:     schema.TypeSet,
							Required: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
							Set:      schema.HashString,
						},
					},
				},
			},
		},
	}
}

func ResourceGroupCreate(d *schema.ResourceData, meta interface{}) error {
	group := nifi.GroupStub()
	group.Revision.Version = 0

	err := GroupFromSchema(meta, d, group)
	if err != nil {
		return fmt.Errorf("Failed to parse User schema")
	}
	parentGroupId := group.Component.ParentGroupId

	// Create user
	client := meta.(*nifi.Client)
	err = client.CreateGroup(group)
	if err != nil {
		return fmt.Errorf("Failed to create Connection")
	}

	// Indicate successful creation
	d.SetId(group.Component.Id)
	d.Set("parent_group_id", parentGroupId)

	return ResourceGroupRead(d, meta)
}

func ResourceGroupRead(d *schema.ResourceData, meta interface{}) error {
	groupId := d.Id()

	client := meta.(*nifi.Client)
	group, err := client.GetGroup(groupId)
	if err != nil {
		return fmt.Errorf("Error retrieving Group: %s", groupId)
	}

	err = GroupToSchema(d, group)
	if err != nil {
		return fmt.Errorf("Failed to serialize Group: %s", groupId)
	}

	return nil
}

func ResourceGroupUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*nifi.Client)
	log.Printf("[INFO] Updating Group: %s..., not implemented", d.Id())
	client.Lock.Lock()
	err := ResourceGroupUpdateInternal(d, meta)
	defer client.Lock.Unlock()
	if err == nil {
		log.Printf("[INFO] Group updated: %s", d.Id())
	} else {
		log.Printf("[ERROR] Group Update failed: %s", d.Id())
	}
	return err
}
func ResourceGroupUpdateInternal(d *schema.ResourceData, meta interface{}) error {
	groupId := d.Id()

	// Refresh group details
	client := meta.(*nifi.Client)
	group, err := client.GetGroup(groupId)
	if "not_found" == err.Error() {
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error retrieving Group: %s", groupId)
	}

	// Load group's desired state
	err = GroupFromSchema(meta, d, group)
	if err != nil {
		return fmt.Errorf("Failed to parse Group schema: %s", groupId)
	}

	// Update group
	err = client.UpdateGroup(group)
	if err != nil {
		return fmt.Errorf("Failed to update Group: %s", groupId)
	}

	return ResourceGroupRead(d, meta)
}

func ResourceGroupDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*nifi.Client)
	log.Printf("[INFO] Deleting Group: %s...", d.Id())
	client.Lock.Lock()
	err := ResourceGroupDeleteInternal(d, meta)
	defer client.Lock.Unlock()
	log.Printf("[INFO] Group deleted: %s", d.Id())
	return err
}

func ResourceGroupDeleteInternal(d *schema.ResourceData, meta interface{}) error {
	groupId := d.Id()

	// Refresh group details
	client := meta.(*nifi.Client)
	group, err := client.GetGroup(groupId)
	if "not_found" == err.Error() {
		d.SetId("")
		return nil
	}
	if err != nil {
		return fmt.Errorf("Error retrieving Group: %s", groupId)
	}

	// Delete group
	err = client.DeleteGroup(group)
	if err != nil {
		return fmt.Errorf("Error deleting Group: %s", groupId)
	}

	d.SetId("")
	return nil
}

func ResourceGroupExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	groupId := d.Id()
	client := meta.(*nifi.Client)
	if groupId != "" {
		_, err := client.GetGroup(groupId)
		if "not_found" == err.Error() {
			log.Printf("[INFO] Group %s no longer exists, removing from state...", groupId)
			d.SetId("")
			return false, nil
		}
		if nil != err {
			return false, fmt.Errorf("Error testing existence of Group: %s", groupId)
		}
	} else {
		v := d.Get("component").([]interface{})
		if len(v) != 1 {
			return false, fmt.Errorf("Exactly one component is required")
		} else {
			component := v[0].(map[string]interface{})
			groupIden := component["identity"].(string)
			if groupIden != "" {
				groupIds, err := client.GetGroupIdsWithIdentity(groupIden)
				if "not_found" == err.Error() {
					log.Printf("[INFO] Group %s no longer exists, removing from state...", groupIden)
					d.SetId("")
					return false, nil
				}
				if nil != err {
					return false, fmt.Errorf("Error testing existence of Group: %s", groupIden)
				}
				if len(groupIds) == 1 {
					d.SetId(groupIds[0])
					return true, nil
				}
				if len(groupIds) > 1 {
					d.SetId("")
					return false, fmt.Errorf("Error more than one Group found with identity: %s", groupIden)
				} else {
					d.SetId("")
					return false, fmt.Errorf("Error testing existence of Group: %s", groupIden)
				}

			} else {
				return false, nil
			}
		}
	}
	return true, nil
}

// Schema Helpers

func GroupFromSchema(meta interface{}, d *schema.ResourceData, group *nifi.Group) error {
	v := d.Get("component").([]interface{})
	if len(v) != 1 {
		return fmt.Errorf("Exactly one component is required")
	}
	component := v[0].(map[string]interface{})
	group.Component.ParentGroupId = component["parent_group_id"].(string)
	group.Component.Identity = component["identity"].(string)

	v = component["position"].([]interface{})
	if len(v) != 1 {
		return fmt.Errorf("Exactly one component.position is required")
	}
	position := v[0].(map[string]interface{})
	group.Component.Position.X = position["x"].(float64)
	group.Component.Position.Y = position["y"].(float64)

	userList := component["users"].(*schema.Set).List()
	tenants := []nifi.Tenant{}
	for _, u := range userList {
		t := nifi.Tenant{Id: u.(string)}
		tenants = append(tenants, t)
	}
	group.Component.Users = tenants
	return nil
}

func GroupToSchema(d *schema.ResourceData, group *nifi.Group) error {
	revision := []map[string]interface{}{{
		"version": group.Revision.Version,
	}}
	d.Set("revision", revision)

	ul := []string{}

	for _, u := range group.Component.Users {
		ul = append(ul, u.Id)
	}
	component := []map[string]interface{}{{
		"parent_group_id": interface{}(group.Component.ParentGroupId).(string),
		"position": []map[string]interface{}{{
			"x": group.Component.Position.X,
			"y": group.Component.Position.Y,
		}},
		"identity": group.Component.Identity,
		"users":    interface{}(ul).([]string),
	}}
	d.Set("component", component)

	return nil
}
