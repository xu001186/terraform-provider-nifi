package nifi

import (
	"fmt"
	"time"
)

// Controller Service section

type ControllerServiceState string

const (
	ControllerServiceState_ENABLED  ControllerServiceState = "ENABLED"
	ControllerServiceState_DISABLED ControllerServiceState = "DISABLED"
)

type ControllerServiceComponent struct {
	Id            string                 `json:"id,omitempty"`
	ParentGroupId string                 `json:"parentGroupId,omitempty"`
	Name          string                 `json:"name,omitempty"`
	Type          string                 `json:"type,omitempty"`
	State         ControllerServiceState `json:"state,omitempty"`
	expectState   ControllerServiceState
	Properties    map[string]interface{} `json:"properties"`
}

type ControllerService struct {
	Revision  Revision                   `json:"revision"`
	Component ControllerServiceComponent `json:"component"`
}

func (c *Client) CreateControllerService(controllerService *ControllerService) error {
	url := fmt.Sprintf("%s/process-groups/%s/controller-services",
		baseurl(c.Config), controllerService.Component.ParentGroupId)
	_, err := c.JsonCall("POST", url, controllerService, controllerService)
	if nil != err {
		return err
	}
	c.CleanupNilProperties(controllerService.Component.Properties)
	return nil
}

func (c *Client) GetControllerService(controllerServiceId string) (*ControllerService, error) {
	url := fmt.Sprintf("%s/controller-services/%s",
		baseurl(c.Config), controllerServiceId)
	controllerService := ControllerService{}
	code, err := c.JsonCall("GET", url, nil, &controllerService)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	c.CleanupNilProperties(controllerService.Component.Properties)
	return &controllerService, nil
}

func (c *Client) UpdateControllerService(controllerService *ControllerService) error {
	url := fmt.Sprintf("%s/controller-services/%s",
		baseurl(c.Config), controllerService.Component.Id)
	_, err := c.JsonCall("PUT", url, controllerService, controllerService)
	if nil != err {
		return err
	}
	c.CleanupNilProperties(controllerService.Component.Properties)
	return nil
}

func (c *Client) DeleteControllerService(controllerService *ControllerService) error {
	url := fmt.Sprintf("%s/controller-services/%s?version=%d",
		baseurl(c.Config), controllerService.Component.Id, controllerService.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

func (c *Client) SetControllerServiceState(controllerService *ControllerService, state ControllerServiceState) error {
	stateUpdate := ControllerService{
		Revision: Revision{
			Version: controllerService.Revision.Version,
		},
		Component: ControllerServiceComponent{
			Id:    controllerService.Component.Id,
			State: state,
		},
	}
	url := fmt.Sprintf("%s/controller-services/%s",
		baseurl(c.Config), controllerService.Component.Id)
	_, err := c.JsonCall("PUT", url, stateUpdate, controllerService)
	return err
}

func (cs *ControllerService) statusCheck(c *Client) bool {
	newcs, err := c.GetControllerService(cs.Component.Id)
	if err != nil {
		return false
	}
	if cs.Component.expectState != newcs.Component.State {
		return false
	}
	return true
}

func (c *Client) EnableControllerService(controllerService *ControllerService) error {
	err := c.SetControllerServiceState(controllerService, ControllerServiceState_ENABLED)
	if err != nil {
		return err
	}
	controllerService.Component.expectState = ControllerServiceState_ENABLED
	return c.WaitUtil(120*time.Second, controllerService.statusCheck)

}

func (c *Client) DisableControllerService(controllerService *ControllerService) error {
	err := c.SetControllerServiceState(controllerService, ControllerServiceState_DISABLED)
	if err != nil {
		return err
	}
	controllerService.Component.expectState = ControllerServiceState_DISABLED
	return c.WaitUtil(120*time.Second, controllerService.statusCheck)
}
