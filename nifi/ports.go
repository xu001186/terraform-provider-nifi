package nifi

import (
	"fmt"
	"log"
	"time"
)

//input port
type Port struct {
	Revision  Revision      `json:"revision"`
	Component PortComponent `json:"component"`
}
type PortComponent struct {
	Id            string    `json:"id,omitempty"`
	ParentGroupId string    `json:"parentGroupId"`
	Name          string    `json:"name"`
	PortType      PortType  `json:"type"`
	Comments      string    `json:"comments"`
	Position      Position  `json:"position"`
	State         PortState `json:"state,omitempty"`
	expectState   PortState
}

type PortStateComponent struct {
	Id    string    `json:"id,omitempty"`
	State PortState `json:"state,omitempty"`
}
type PortStateUpdate struct {
	Revision  Revision           `json:"revision"`
	Component PortStateComponent `json:"component"`
}

type PortType string
type PortState string

const (
	PortType_INPUT_PORT  PortType  = "INPUT_PORT"
	PortType_OUTPUT_PORT PortType  = "OUTPUT_PORT"
	PortState_RUNNING    PortState = "RUNNING"
	PortState_STOPPED    PortState = "STOPPED"
	PortState_DISABLED   PortState = "DISABLED"
)

func (c *Client) CreatePort(port *Port) error {
	parent_group_id := port.Component.ParentGroupId
	port_type := port.Component.PortType
	url := ""
	switch port_type {
	case PortType_INPUT_PORT:
		url = fmt.Sprintf("%s/process-groups/%s/input-ports",
			baseurl(c.Config), parent_group_id)
	case PortType_OUTPUT_PORT:
		url = fmt.Sprintf("%s/process-groups/%s/output-ports",
			baseurl(c.Config), parent_group_id)
	default:
		return fmt.Errorf("invalid port type : %s", string(port_type))
	}
	_, err := c.JsonCall("POST", url, port, port)
	return err
}

func (c *Client) UpdatePort(port *Port) error {
	port_type := port.Component.PortType
	portId := port.Component.Id
	url := ""
	switch port_type {
	case PortType_INPUT_PORT:
		url = fmt.Sprintf("%s/input-ports/%s",
			baseurl(c.Config), portId)
	case PortType_OUTPUT_PORT:
		url = fmt.Sprintf("%s/output-ports/%s",
			baseurl(c.Config), portId)
	default:
		return fmt.Errorf("invalid port type : %s", string(port_type))
	}
	responseCode, err := c.JsonCall("PUT", url, port, port)
	if responseCode == 409 {
		log.Printf("[WARN]: port not updated, since it's not invalid state")
	}
	return err
}
func (c *Client) GetPort(portId string, port_type PortType) (*Port, error) {
	url := ""
	switch port_type {
	case PortType_INPUT_PORT:
		url = fmt.Sprintf("%s/input-ports/%s",
			baseurl(c.Config), portId)
	case PortType_OUTPUT_PORT:
		url = fmt.Sprintf("%s/output-ports/%s",
			baseurl(c.Config), portId)
	default:
		return nil, fmt.Errorf("invalid port type : %s", string(port_type))
	}
	port := Port{}
	code, err := c.JsonCall("GET", url, nil, &port)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	return &port, nil
}

func (c *Client) DeletePort(port *Port) error {
	port_id := port.Component.Id
	port_type := port.Component.PortType
	url := ""
	switch port_type {
	case PortType_INPUT_PORT:
		url = fmt.Sprintf("%s/input-ports/%s?version=%d",
			baseurl(c.Config), port_id, port.Revision.Version)
	case PortType_OUTPUT_PORT:
		url = fmt.Sprintf("%s/output-ports/%s?version=%d",
			baseurl(c.Config), port_id, port.Revision.Version)
	default:
		return fmt.Errorf("invalid port type : %s", string(port_type))
	}
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

func (p *Port) statusCheck(c *Client) bool {
	port, err := c.GetPort(p.Component.Id, p.Component.PortType)
	if err != nil {
		return false
	}
	if port.Component.State != p.Component.expectState {
		return false
	}
	return true
}

func (c *Client) SetPortState(port *Port, state PortState) error {
	log.Printf("[Info] Set port to state %s", state)
	//https://community.hortonworks.com/questions/67900/startstop-processor-via-nifi-api.html
	stateUpdate := PortStateUpdate{
		Revision: Revision{
			Version: port.Revision.Version,
		},
		Component: PortStateComponent{
			Id:    port.Component.Id,
			State: state,
		},
	}

	port_type := port.Component.PortType
	portId := port.Component.Id
	url := ""
	switch port_type {
	case PortType_INPUT_PORT:
		url = fmt.Sprintf("%s/input-ports/%s",
			baseurl(c.Config), portId)
	case PortType_OUTPUT_PORT:
		url = fmt.Sprintf("%s/output-ports/%s",
			baseurl(c.Config), portId)
	default:
		return fmt.Errorf("invalid port type : %s", string(port_type))
	}

	responseCode, err := c.JsonCall("PUT", url, stateUpdate, port)
	if err != nil {
		if responseCode == 409 {
			// if 409, same state
			log.Printf("[WARN]: 409 %s", err)
			err = nil
		} else {
			log.Printf("[Fatal]: Failed to set state of  Port, error code %s.", err)
			return err
		}
	}
	port.Component.expectState = state
	return c.WaitUtil(120*time.Second, port.statusCheck)
}

func (c *Client) StartPort(port *Port) error {
	return c.SetPortState(port, PortState_RUNNING)
}

func (c *Client) StopPort(port *Port) error {
	return c.SetPortState(port, PortState_STOPPED)
}

func (c *Client) DisablePort(port *Port) error {
	return c.SetPortState(port, PortState_DISABLED)
}
