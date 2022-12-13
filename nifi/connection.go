package nifi

import (
	"fmt"
	"log"
	"time"
)

type ConnectionHand_Type string

const (
	ConnectionHand_Type_PROCESSOR          ConnectionHand_Type = "PROCESSOR"
	ConnectionHand_Type_REMOTE_INPUT_PORT  ConnectionHand_Type = "REMOTE_INPUT_PORT"
	ConnectionHand_Type_REMOTE_OUTPUT_PORT ConnectionHand_Type = "REMOTE_OUTPUT_PORT"
	ConnectionHand_Type_INPUT_PORT         ConnectionHand_Type = "INPUT_PORT"
	ConnectionHand_Type_OUTPUT_PORT        ConnectionHand_Type = "OUTPUT_PORT"
	ConnectionHand_Type_FUNNEL             ConnectionHand_Type = "FUNNEL"
)

// Connection section

type ConnectionHand struct {
	Type    ConnectionHand_Type `json:"type"`
	Id      string              `json:"id"`
	GroupId string              `json:"groupId"`
}

type ConnectionComponent struct {
	Id                            string         `json:"id,omitempty"`
	ParentGroupId                 string         `json:"parentGroupId"`
	BackPressureDataSizeThreshold string         `json:"backPressureDataSizeThreshold"`
	BackPressureObjectThreshold   int            `json:"backPressureObjectThreshold"`
	Source                        ConnectionHand `json:"source"`
	Destination                   ConnectionHand `json:"destination"`
	SelectedRelationships         []string       `json:"selectedRelationships"`
	Bends                         []Position     `json:"bends"`
}

type Connection struct {
	Revision  Revision            `json:"revision"`
	Component ConnectionComponent `json:"component"`
}

type Connections struct {
	Connections []Connection `json:"connections"`
}

type ConnectionDropRequest struct {
	DropRequest struct {
		Id       string `json:"id"`
		Finished bool   `json:"finished"`
	} `json:"dropRequest"`
}

func (c *Client) CreateConnection(connection *Connection) error {
	url := fmt.Sprintf("%s/process-groups/%s/connections",
		baseurl(c.Config), connection.Component.ParentGroupId)

	_, err := c.JsonCall("POST", url, connection, connection)
	return err
}

func (c *Client) GetConnection(connectionId string) (*Connection, error) {
	url := fmt.Sprintf("%s/connections/%s",
		baseurl(c.Config), connectionId)
	connection := Connection{}
	code, err := c.JsonCall("GET", url, nil, &connection)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	return &connection, nil
}

func (c *Client) UpdateConnection(connection *Connection) error {
	url := fmt.Sprintf("%s/connections/%s",
		baseurl(c.Config), connection.Component.Id)
	_, err := c.JsonCall("PUT", url, connection, connection)
	return err
}

func (c *Client) DeleteConnection(connection *Connection) error {
	url := fmt.Sprintf("%s/connections/%s?version=%d",
		baseurl(c.Config), connection.Component.Id, connection.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

func (c *Client) DropConnectionData(connection *Connection) error {
	// Create a request to drop the contents of the queue in this connection
	url := fmt.Sprintf("%s/flowfile-queues/%s/drop-requests",
		baseurl(c.Config), connection.Component.Id)
	dropRequest := ConnectionDropRequest{}
	_, err := c.JsonCall("POST", url, nil, &dropRequest)
	if nil != err {
		return err
	}

	// Give it some time to complete
	maxAttempts := 10
	for iteration := 0; iteration < maxAttempts; iteration++ {
		// Check status of the request
		url = fmt.Sprintf("%s/flowfile-queues/%s/drop-requests/%s",
			baseurl(c.Config), connection.Component.Id, dropRequest.DropRequest.Id)
		_, err = c.JsonCall("GET", url, nil, &dropRequest)
		if nil != err {
			continue
		}
		if dropRequest.DropRequest.Finished {
			break
		}

		// Log progress
		log.Printf("[INFO] Purging Connection data %s %d...", dropRequest.DropRequest.Id, iteration+1)

		// Wait a bit
		time.Sleep(3 * time.Second)

		if maxAttempts-1 == iteration {
			log.Printf("[INFO] Failed to purge the Connection %s", dropRequest.DropRequest.Id)
		}
	}

	// Remove a request to drop the contents of this connection
	url = fmt.Sprintf("%s/flowfile-queues/%s/drop-requests/%s",
		baseurl(c.Config), connection.Component.Id, dropRequest.DropRequest.Id)
	_, err = c.JsonCall("DELETE", url, nil, nil)
	if nil != err {
		return err
	}

	return nil
}
