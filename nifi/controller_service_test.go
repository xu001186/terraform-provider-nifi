package nifi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientControllerService(t *testing.T) {

	client := setup()

	processGroup := ProcessGroup{
		Revision: Revision{
			Version: 0,
		},
		Component: ProcessGroupComponent{
			ParentGroupId: "root",
			Name:          "aws_test",
			Position: Position{
				X: 0,
				Y: 0,
			},
		},
	}
	err := client.CreateProcessGroup(&processGroup)
	assert.Nil(t, err)
	assert.NotEmpty(t, processGroup.Component.Id)

	controllerService := ControllerService{
		Revision: Revision{
			Version: 0,
		},
		Component: ControllerServiceComponent{
			ParentGroupId: processGroup.Component.Id,
			Name:          "aws_controller",
			Type:          "org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService",
			State:         "ENABLED",
		},
	}
	err = client.CreateControllerService(&controllerService)
	assert.Nil(t, err)
	assert.NotEmpty(t, controllerService.Component.Id)

	err = client.DisableControllerService(&controllerService)
	assert.Nil(t, err)
	assert.Equal(t, controllerService.Component.State, ControllerServiceState_DISABLED)

	err = client.EnableControllerService(&controllerService)
	assert.Nil(t, err)
	assert.Equal(t, controllerService.Component.State, ControllerServiceState_ENABLED)

	err = client.DisableControllerService(&controllerService)
	assert.Nil(t, err)
	assert.Equal(t, controllerService.Component.State, ControllerServiceState_DISABLED)

	err = client.DeleteControllerService(&controllerService)
	assert.Nil(t, err)

	client.DeleteProcessGroup(&processGroup)
	assert.Nil(t, err)
}
