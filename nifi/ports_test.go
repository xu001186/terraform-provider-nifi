package nifi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientInputPort(t *testing.T) {
	client := setup()

	inputPort := Port{
		Revision: Revision{
			Version: 0,
		},
		Component: PortComponent{
			ParentGroupId: "root",
			Name:          "test_input_port",
			Position: Position{
				X: 0,
				Y: 100,
			},
			PortType: PortType_INPUT_PORT,
		},
	}
	client.CreatePort(&inputPort)
	assert.NotEmpty(t, inputPort.Component.Id)

	inputPort2, err := client.GetPort(inputPort.Component.Id, inputPort.Component.PortType)
	assert.Equal(t, err, nil)
	assert.NotEmpty(t, inputPort2.Component.Id)

	inputPort.Component.Name = "test_input_port2"
	err = client.UpdatePort(&inputPort)
	assert.Equal(t, err, nil)

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
				Y: 200,
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
				Id:      inputPort.Component.Id,
				Type:    ConnectionHand_Type_INPUT_PORT,
				GroupId: "root",
			},
			Destination: ConnectionHand{
				Id:      processor2.Component.Id,
				Type:    ConnectionHand_Type_PROCESSOR,
				GroupId: "root",
			},
		},
	}
	err = client.CreateConnection(&connection)
	assert.Nil(t, err)
	assert.NotEmpty(t, connection.Component.Id)

	err = client.StartPort(&inputPort)
	assert.Nil(t, err)
	assert.Equal(t, inputPort.Component.State, PortState_RUNNING)

	err = client.StopPort(&inputPort)
	assert.Nil(t, err)
	assert.Equal(t, inputPort.Component.State, PortState_STOPPED)

	err = client.DisablePort(&inputPort)
	assert.Nil(t, err)
	assert.Equal(t, inputPort.Component.State, PortState_DISABLED)

	err = client.DeleteConnection(&connection)
	assert.Equal(t, err, nil)

	err = client.DeleteProcessor(&processor2)
	assert.Equal(t, err, nil)

	err = client.DeletePort(&inputPort)
	assert.Equal(t, err, nil)

}

func TestClientOutputPort(t *testing.T) {

	client := setup()

	outputPort := Port{
		Revision: Revision{
			Version: 0,
		},
		Component: PortComponent{
			ParentGroupId: "root",
			Name:          "test_output_port",
			Position: Position{
				X: 0,
				Y: 100,
			},
			PortType: PortType_OUTPUT_PORT,
		},
	}
	client.CreatePort(&outputPort)
	assert.NotEmpty(t, outputPort.Component.Id)

	outputPort2, err := client.GetPort(outputPort.Component.Id, outputPort.Component.PortType)
	assert.Equal(t, err, nil)
	assert.NotEmpty(t, outputPort2.Component.Id)

	outputPort.Component.Name = "test_output_port2"
	err = client.UpdatePort(&outputPort)
	assert.Equal(t, err, nil)

	processor2 := Processor{
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
			Destination: ConnectionHand{
				Id:      outputPort2.Component.Id,
				Type:    ConnectionHand_Type_OUTPUT_PORT,
				GroupId: "root",
			},
			Source: ConnectionHand{
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

	err = client.StartPort(&outputPort)
	assert.Nil(t, err)
	assert.Equal(t, outputPort.Component.State, PortState_RUNNING)

	err = client.StopPort(&outputPort)
	assert.Nil(t, err)
	assert.Equal(t, outputPort.Component.State, PortState_STOPPED)

	err = client.DisablePort(&outputPort)
	assert.Nil(t, err)
	assert.Equal(t, outputPort.Component.State, PortState_DISABLED)

	err = client.DeleteConnection(&connection)
	assert.Equal(t, err, nil)

	err = client.DeleteProcessor(&processor2)
	assert.Equal(t, err, nil)

	err = client.DeletePort(&outputPort)
	assert.Equal(t, err, nil)

}
