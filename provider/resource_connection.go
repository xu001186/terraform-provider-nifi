package provider

import (
	"fmt"
	"log"

	nifi "github.com/glympse/terraform-provider-nifi/nifi"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ResourceConnection() *schema.Resource {
	return &schema.Resource{
		Create: ResourceConnectionCreate,
		Read:   ResourceConnectionRead,
		Update: ResourceConnectionUpdate,
		Delete: ResourceConnectionDelete,
		Exists: ResourceConnectionExists,

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
						"back_pressure_data_size_threshold": {
							Type:     schema.TypeString,
							Optional: true,
							Default:  "1 GB",
						},
						"back_pressure_object_threshold": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  10000,
						},
						"source": {
							Type:     schema.TypeList,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"type": {
										Type:     schema.TypeString,
										Required: true,
									},
									"id": {
										Type:     schema.TypeString,
										Required: true,
									},
									"group_id": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
						"destination": {
							Type:     schema.TypeList,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"type": {
										Type:     schema.TypeString,
										Required: true,
									},
									"id": {
										Type:     schema.TypeString,
										Required: true,
									},
									"group_id": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
						"selected_relationships": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"bends": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"x": {
										Type:     schema.TypeFloat,
										Required: true,
									},
									"y": {
										Type:     schema.TypeFloat,
										Required: true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func ResourceConnectionCreate(d *schema.ResourceData, meta interface{}) error {
	connection := nifi.Connection{}
	connection.Revision.Version = 0
	err := ConnectionFromSchema(d, &connection)
	if err != nil {
		return fmt.Errorf("failed to parse Connection schema")
	}
	parentGroupId := connection.Component.ParentGroupId

	// Create connection
	client := meta.(*nifi.Client)
	err = client.CreateConnection(&connection)
	if err != nil {
		return fmt.Errorf("failed to create Connection %s", err)
	}
	client.StartConnectionHand(&connection.Component.Source)
	client.StartConnectionHand(&connection.Component.Destination)
	// Indicate successful creation
	d.SetId(connection.Component.Id)
	d.Set("parent_group_id", parentGroupId)

	return ResourceConnectionRead(d, meta)
}

func ResourceConnectionRead(d *schema.ResourceData, meta interface{}) error {
	connectionId := d.Id()

	client := meta.(*nifi.Client)
	connection, err := client.GetConnection(connectionId)
	if err != nil {
		return fmt.Errorf("error retrieving Connection: %s", connectionId)
	}

	err = ConnectionToSchema(d, connection)
	if err != nil {
		return fmt.Errorf("failed to serialize Connection: %s", connectionId)
	}

	return nil
}

func ResourceConnectionUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*nifi.Client)
	client.Lock.Lock()
	log.Printf("[INFO] Updating Connection: %s...", d.Id())
	err := ResourceConnectionUpdateInternal(d, meta)
	defer client.Lock.Unlock()
	if err == nil {
		log.Printf("[INFO] Connection updated: %s", d.Id())
	} else {
		log.Printf("[ERROR] Connection update failed: %s", d.Id())
	}
	return err
}

func ResourceConnectionUpdateInternal(d *schema.ResourceData, meta interface{}) error {
	connectionId := d.Id()

	// Refresh connection details
	client := meta.(*nifi.Client)
	connection, err := client.GetConnection(connectionId)
	if err != nil {
		if err.Error() == "not_found" {
			d.SetId("")
			return nil
		} else {
			return fmt.Errorf("error retrieving Connection: %s", connectionId)
		}
	}

	// Stop related processors
	err = client.StopConnectionHand(&connection.Component.Source)
	if err != nil {
		return fmt.Errorf("failed to stop source Processor: %s", connection.Component.Source.Id)
	}
	err = client.StopConnectionHand(&connection.Component.Destination)
	if err != nil {
		return fmt.Errorf("failed to stop destination Processor: %s", connection.Component.Destination.Id)
	}

	// Update connection
	err = ConnectionFromSchema(d, connection)
	if err != nil {
		return fmt.Errorf("failed to parse Connection schema: %s", connectionId)
	}
	err = client.UpdateConnection(connection)
	if err != nil {
		return fmt.Errorf("failed to update Connection: %s", connectionId)
	}

	// Start related processors

	client.StartConnectionHand(&connection.Component.Source)
	client.StartConnectionHand(&connection.Component.Destination)

	return ResourceConnectionRead(d, meta)
}

func ResourceConnectionDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*nifi.Client)
	client.Lock.Lock()
	log.Printf("[INFO] Deleting Connection: %s...", d.Id())
	err := ResourceConnectionDeleteInternal(d, meta)
	defer client.Lock.Unlock()
	if err != nil {
		log.Printf("[ERROR] Connection deletion failed: %s", d.Id())
	} else {
		log.Printf("[Info] Connection deleted: %s", d.Id())
	}
	return err
}

func ResourceConnectionDeleteInternal(d *schema.ResourceData, meta interface{}) error {
	connectionId := d.Id()

	// Refresh connection details
	client := meta.(*nifi.Client)
	connection, err := client.GetConnection(connectionId)
	if err != nil {
		if err.Error() == "not_found" {
			d.SetId("")
			return nil
		} else {
			return fmt.Errorf("error retrieving Connection: %s", connectionId)
		}
	}
	source := &connection.Component.Source
	destination := &connection.Component.Destination
	// Stop related processors if it is started
	err = client.StopConnectionHand(source)
	if err != nil {
		return fmt.Errorf("failed to stop source Processor: %s", connection.Component.Source.Id)
	}

	err = client.StopConnectionHand(destination)
	if err != nil {
		return fmt.Errorf("failed to stop destination Processor: %s", connection.Component.Destination.Id)
	}

	// Purge connection data
	log.Printf("[INFO] Dropping connection data: %d", connection.Revision.Version)
	err = client.DropConnectionData(connection)
	if nil != err {
		return fmt.Errorf("error purging Connection: %s", connectionId)
	}

	// Delete connection
	// refresh conneciton so that the source/dest running status passing check
	connection, err = client.GetConnection(connectionId)
	if err != nil {
		return fmt.Errorf("error read Connection: %s", connectionId)
	}
	err = client.DeleteConnection(connection)
	if err != nil {
		return fmt.Errorf("error deleting Connection: %s", connectionId)
	}

	// Start related processors
	client.StartConnectionHand(source)
	client.StartConnectionHand(destination)

	d.SetId("")
	return nil
}

func ResourceConnectionExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	connectionId := d.Id()

	client := meta.(*nifi.Client)
	_, err := client.GetConnection(connectionId)
	if nil != err {
		if err.Error() == "not_found" {
			log.Printf("[INFO] Connection %s no longer exists, removing from state...", connectionId)
			d.SetId("")
			return false, nil
		} else {
			return false, fmt.Errorf("error testing existence of Connection: %s", connectionId)
		}
	}

	return true, nil
}

// Schema Helpers

func ConnectionFromSchema(d *schema.ResourceData, connection *nifi.Connection) error {
	v := d.Get("component").([]interface{})
	if len(v) != 1 {
		return fmt.Errorf("exactly one component is required")
	}
	component := v[0].(map[string]interface{})
	connection.Component.ParentGroupId = component["parent_group_id"].(string)

	connection.Component.BackPressureDataSizeThreshold = component["back_pressure_data_size_threshold"].(string)
	connection.Component.BackPressureObjectThreshold = component["back_pressure_object_threshold"].(int)

	v = component["source"].([]interface{})
	if len(v) != 1 {
		return fmt.Errorf("exactly one component.source is required")
	}
	source := v[0].(map[string]interface{})
	connection.Component.Source.Type = source["type"].(nifi.ConnectionHand_Type)
	connection.Component.Source.Id = source["id"].(string)
	connection.Component.Source.GroupId = source["group_id"].(string)

	v = component["destination"].([]interface{})
	if len(v) != 1 {
		return fmt.Errorf("exactly one component.destination is required")
	}
	destination := v[0].(map[string]interface{})
	connection.Component.Destination.Type = destination["type"].(nifi.ConnectionHand_Type)
	connection.Component.Destination.Id = destination["id"].(string)
	connection.Component.Destination.GroupId = destination["group_id"].(string)

	selectedRelationships := []string{}
	relationships := component["selected_relationships"].([]interface{})
	for _, v := range relationships {
		selectedRelationships = append(selectedRelationships, v.(string))
	}
	connection.Component.SelectedRelationships = selectedRelationships

	v = component["bends"].([]interface{})
	if len(v) > 0 {
		bends := []nifi.Position{}
		for _, vv := range v {
			bend := vv.(map[string]interface{})
			bends = append(bends, nifi.Position{
				X: bend["x"].(float64),
				Y: bend["y"].(float64),
			})
		}
		connection.Component.Bends = bends
	}

	return nil
}

func ConnectionToSchema(d *schema.ResourceData, connection *nifi.Connection) error {
	revision := []map[string]interface{}{{
		"version": connection.Revision.Version,
	}}
	d.Set("revision", revision)

	relationships := []interface{}{}
	for _, v := range connection.Component.SelectedRelationships {
		relationships = append(relationships, v)
	}

	bends := []interface{}{}
	for _, v := range connection.Component.Bends {
		bends = append(bends, map[string]interface{}{
			"x": v.X,
			"y": v.Y,
		})
	}

	component := []map[string]interface{}{{
		"parent_group_id":                   d.Get("parent_group_id").(string),
		"back_pressure_data_size_threshold": connection.Component.BackPressureDataSizeThreshold,
		"back_pressure_object_threshold":    connection.Component.BackPressureObjectThreshold,
		"source": []map[string]interface{}{{
			"type":     connection.Component.Source.Type,
			"id":       connection.Component.Source.Id,
			"group_id": connection.Component.Source.GroupId,
		}},
		"destination": []map[string]interface{}{{
			"type":     connection.Component.Destination.Type,
			"id":       connection.Component.Destination.Id,
			"group_id": connection.Component.Destination.GroupId,
		}},
		"selected_relationships": relationships,
		"bends":                  bends,
	}}
	d.Set("component", component)

	return nil
}
