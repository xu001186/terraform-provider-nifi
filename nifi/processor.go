package nifi

import (
	"fmt"
)

// Processor section

type ProcessorRelationship struct {
	Name          string `json:"name"`
	AutoTerminate bool   `json:"autoTerminate"`
}

type ExecutionNode string
type SchedulingStrategy string

const (
	ExecutionNode_ALL               ExecutionNode      = "ALL"
	ExecutionNode_PRIMARY           ExecutionNode      = "PRIMARY"
	SchedulingStrategy_TIMER_DRIVEN SchedulingStrategy = "TIMER_DRIVEN"
	SchedulingStrategy_CRON_DRIVEN  SchedulingStrategy = "CRON_DRIVEN"
)

type ProcessorConfig struct {
	SchedulingStrategy               SchedulingStrategy     `json:"schedulingStrategy"`
	SchedulingPeriod                 string                 `json:"schedulingPeriod"`
	ExecutionNode                    ExecutionNode          `json:"executionNode"`
	ConcurrentlySchedulableTaskCount int                    `json:"concurrentlySchedulableTaskCount"`
	Properties                       map[string]interface{} `json:"properties"`
	AutoTerminatedRelationships      []string               `json:"autoTerminatedRelationships"`
}

type ProcessorComponent struct {
	Id            string                  `json:"id,omitempty"`
	ParentGroupId string                  `json:"parentGroupId,omitempty"`
	Name          string                  `json:"name,omitempty"`
	Type          string                  `json:"type,omitempty"`
	Position      *Position               `json:"position,omitempty"`
	State         string                  `json:"state,omitempty"`
	Config        *ProcessorConfig        `json:"config,omitempty"`
	Relationships []ProcessorRelationship `json:"relationships,omitempty"`
}

type Processor struct {
	Revision  Revision           `json:"revision"`
	Component ProcessorComponent `json:"component"`
}

func ProcessorStub() *Processor {
	return &Processor{
		Component: ProcessorComponent{
			Position: &Position{},
			Config:   &ProcessorConfig{},
		},
	}
}

func (c *Client) CleanupNilProperties(properties map[string]interface{}) error {
	for k, v := range properties {
		if v == nil {
			delete(properties, k)
		}
	}
	return nil
}

func (c *Client) CreateProcessor(processor *Processor) error {
	url := fmt.Sprintf("%s/process-groups/%s/processors",
		baseurl(c.Config), processor.Component.ParentGroupId)
	_, err := c.JsonCall("POST", url, processor, processor)
	if nil != err {
		return err
	}
	c.CleanupNilProperties(processor.Component.Config.Properties)
	return nil
}

func (c *Client) GetProcessor(processorId string) (*Processor, error) {
	url := fmt.Sprintf("%s/processors/%s",
		baseurl(c.Config), processorId)
	processor := ProcessorStub()
	code, err := c.JsonCall("GET", url, nil, &processor)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}

	c.CleanupNilProperties(processor.Component.Config.Properties)

	relationships := []string{}
	for _, v := range processor.Component.Relationships {
		if v.AutoTerminate {
			relationships = append(relationships, v.Name)
		}
	}
	processor.Component.Config.AutoTerminatedRelationships = relationships

	return processor, nil
}

func (c *Client) UpdateProcessor(processor *Processor) error {
	url := fmt.Sprintf("%s/processors/%s",
		baseurl(c.Config), processor.Component.Id)
	_, err := c.JsonCall("PUT", url, processor, processor)
	if nil != err {
		return err
	}
	c.CleanupNilProperties(processor.Component.Config.Properties)
	return nil
}

func (c *Client) DeleteProcessor(processor *Processor) error {
	url := fmt.Sprintf("%s/processors/%s?version=%d",
		baseurl(c.Config), processor.Component.Id, processor.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

func (c *Client) SetProcessorState(processor *Processor, state string) error {
	stateUpdate := Processor{
		Revision: Revision{
			Version: processor.Revision.Version,
		},
		Component: ProcessorComponent{
			Id:    processor.Component.Id,
			State: state,
		},
	}
	url := fmt.Sprintf("%s/processors/%s",
		baseurl(c.Config), processor.Component.Id)
	_, err := c.JsonCall("PUT", url, stateUpdate, processor)
	return err
}

func (c *Client) StartProcessor(processor *Processor) error {
	return c.SetProcessorState(processor, "RUNNING")
}

func (c *Client) StopProcessor(processor *Processor) error {
	return c.SetProcessorState(processor, "STOPPED")
}
