package nifi

import "fmt"

// Process Group section

type ProcessGroupComponent struct {
	Id            string   `json:"id,omitempty"`
	ParentGroupId string   `json:"parentGroupId"`
	Name          string   `json:"name"`
	Position      Position `json:"position"`
}

type ProcessGroup struct {
	Revision  Revision              `json:"revision"`
	Component ProcessGroupComponent `json:"component"`
}

func (c *Client) CreateProcessGroup(processGroup *ProcessGroup) error {
	url := fmt.Sprintf("%s/process-groups/%s/process-groups",
		baseurl(c.Config), processGroup.Component.ParentGroupId)
	_, err := c.JsonCall("POST", url, processGroup, processGroup)
	return err
}

func (c *Client) GetProcessGroup(processGroupId string) (*ProcessGroup, error) {
	url := fmt.Sprintf("%s/process-groups/%s",
		baseurl(c.Config), processGroupId)
	processGroup := ProcessGroup{}
	code, err := c.JsonCall("GET", url, nil, &processGroup)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	return &processGroup, nil
}

func (c *Client) UpdateProcessGroup(processGroup *ProcessGroup) error {
	url := fmt.Sprintf("%s/process-groups/%s",
		baseurl(c.Config), processGroup.Component.Id)
	_, err := c.JsonCall("PUT", url, processGroup, processGroup)
	return err
}

func (c *Client) DeleteProcessGroup(processGroup *ProcessGroup) error {
	url := fmt.Sprintf("%s/process-groups/%s?version=%d",
		baseurl(c.Config), processGroup.Component.Id, processGroup.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

func (c *Client) GetProcessGroupConnections(processGroupId string) (*Connections, error) {
	url := fmt.Sprintf("%s/process-groups/%s/connections",
		baseurl(c.Config), processGroupId)
	connections := Connections{}
	_, err := c.JsonCall("GET", url, nil, &connections)
	if nil != err {
		return nil, err
	}
	return &connections, nil
}
