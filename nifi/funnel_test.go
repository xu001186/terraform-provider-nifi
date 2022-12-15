package nifi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunnel(t *testing.T) {

	client := setup()

	funnel := Funnel{
		Revision: Revision{
			Version: 0,
		},
		Component: FunnelComponent{
			ParentGroupId: "root",
			Position: Position{
				X: 0,
				Y: 0,
			},
		},
	}
	err := client.CreateFunnel(&funnel)
	assert.Nil(t, err)
	assert.NotEmpty(t, funnel.Component.Id)
	getFunnel, err := client.GetFunnel(funnel.Component.Id)
	assert.Nil(t, err)
	assert.Equal(t, getFunnel.Component.Id, funnel.Component.Id)
	funnel.Component.Position.X = 10
	err = client.UpdateFunnel(&funnel)
	assert.Nil(t, err)
	assert.Equal(t, funnel.Component.Position.X, float64(10))
	err = client.DeleteFunnel(&funnel)
	assert.Nil(t, err)

}
