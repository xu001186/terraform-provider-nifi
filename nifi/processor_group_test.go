package nifi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientProcessGroup(t *testing.T) {
	client := setup()
	processGroup := ProcessGroup{
		Revision: Revision{
			Version: 0,
		},
		Component: ProcessGroupComponent{
			ParentGroupId: "root",
			Name:          "kafka_to_s3_2",
			Position: Position{
				X: 0,
				Y: 5,
			},
		},
	}
	err := client.CreateProcessGroup(&processGroup)
	assert.Equal(t, err, nil)
	assert.NotEmpty(t, processGroup.Component.Id)

	processGroup2, err := client.GetProcessGroup(processGroup.Component.Id)
	assert.Equal(t, err, nil)
	assert.NotEmpty(t, processGroup2.Component.Id)

	processGroup.Component.Name = "kafka_to_s3_5"
	err = client.UpdateProcessGroup(&processGroup)
	assert.Equal(t, err, nil)

	err = client.DeleteProcessGroup(&processGroup)
	assert.Equal(t, err, nil)
}
