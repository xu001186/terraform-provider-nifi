package nifi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientProcessor(t *testing.T) {
	client := setup()

	processor := Processor{
		Revision: Revision{
			Version: 0,
		},
		Component: ProcessorComponent{
			ParentGroupId: "root",
			Name:          "generate_flowfile",
			Type:          "org.apache.nifi.processors.standard.GenerateFlowFile",
			Position: &Position{
				X: 0,
				Y: 0,
			},
			Config: &ProcessorConfig{
				ExecutionNode:                    ExecutionNode_ALL,
				SchedulingStrategy:               SchedulingStrategy_TIMER_DRIVEN,
				SchedulingPeriod:                 "0 sec",
				ConcurrentlySchedulableTaskCount: 1,
				Properties: map[string]interface{}{
					"File Size":        "0B",
					"Batch Size":       "1",
					"Data Format":      "Text",
					"Unique FlowFiles": "false",
				},
				AutoTerminatedRelationships: []string{
					"success",
				},
			},
		},
	}
	err := client.CreateProcessor(&processor)
	assert.Nil(t, err)
	assert.NotEmpty(t, processor.Component.Id)

	processor.Component.Config.AutoTerminatedRelationships = []string{}
	err = client.UpdateProcessor(&processor)
	assert.Nil(t, err)

	processor.Component.Config.AutoTerminatedRelationships = []string{
		"success",
	}
	err = client.UpdateProcessor(&processor)
	assert.Nil(t, err)

	err = client.StartProcessor(&processor)
	assert.Nil(t, err)

	err = client.StopProcessor(&processor)
	assert.Nil(t, err)

	err = client.DeleteProcessor(&processor)
	assert.Nil(t, err)
}
