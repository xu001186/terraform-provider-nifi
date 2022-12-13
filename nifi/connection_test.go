package nifi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientConnection(t *testing.T) {
	client := setup()

	processor1 := Processor{
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
				SchedulingStrategy:               SchedulingStrategy_TIMER_DRIVEN,
				ExecutionNode:                    ExecutionNode_ALL,
				SchedulingPeriod:                 "0 sec",
				ConcurrentlySchedulableTaskCount: 1,
				Properties: map[string]interface{}{
					"File Size":        "0B",
					"Batch Size":       "1",
					"Data Format":      "Text",
					"Unique FlowFiles": "false",
				},
				AutoTerminatedRelationships: []string{},
			},
		},
	}
	err := client.CreateProcessor(&processor1)
	assert.Nil(t, err)
	assert.NotEmpty(t, processor1.Component.Id)

	processor2 := Processor{
		Revision: Revision{
			Version: 0,
		},
		Component: ProcessorComponent{
			ParentGroupId: "root",
			Name:          "wait",
			Type:          "org.apache.nifi.processors.standard.Wait",
			Position: &Position{
				X: 0,
				Y: 0,
			},
			Config: &ProcessorConfig{
				SchedulingStrategy:               SchedulingStrategy_TIMER_DRIVEN,
				ExecutionNode:                    ExecutionNode_ALL,
				SchedulingPeriod:                 "0 sec",
				ConcurrentlySchedulableTaskCount: 1,
				Properties:                       map[string]interface{}{},
				AutoTerminatedRelationships: []string{
					"success",
				},
			},
		},
	}
	err = client.CreateProcessor(&processor2)
	assert.Nil(t, err)
	assert.NotEmpty(t, processor2.Component.Id)

	connection := Connection{
		Revision: Revision{
			Version: 0,
		},
		Component: ConnectionComponent{
			ParentGroupId:                 "root",
			BackPressureDataSizeThreshold: "1 GB",
			BackPressureObjectThreshold:   1000,
			Source: ConnectionHand{
				Id:      processor1.Component.Id,
				Type:    ConnectionHand_Type_PROCESSOR,
				GroupId: "root",
			},
			Destination: ConnectionHand{
				Id:      processor2.Component.Id,
				Type:    ConnectionHand_Type_PROCESSOR,
				GroupId: "root",
			},
			SelectedRelationships: []string{
				"success",
			},
		},
	}
	err = client.CreateConnection(&connection)
	assert.Nil(t, err)
	assert.NotEmpty(t, connection.Component.Id)
	connection.Component.BackPressureObjectThreshold = 2000

	err = client.UpdateConnection(&connection)
	assert.Nil(t, err)
	assert.Equal(t, 2000, connection.Component.BackPressureObjectThreshold)

	err = client.DeleteConnection(&connection)
	assert.Nil(t, err)

	err = client.DeleteProcessor(&processor1)
	assert.Nil(t, err)
	err = client.DeleteProcessor(&processor2)
	assert.Nil(t, err)
}
