package nifi

import "fmt"

type FunnelComponent struct {
	Id            string   `json:"id,omitempty"`
	ParentGroupId string   `json:"parentGroupId,omitempty"`
	Position      Position `json:"position,omitempty"`
}

type Funnel struct {
	Revision  Revision        `json:"revision"`
	Component FunnelComponent `json:"component"`
}

func FunnelStub() *Funnel {
	return &Funnel{
		Component: FunnelComponent{
			Position: Position{},
		},
	}
}
func (c *Client) CreateFunnel(funel *Funnel) error {
	url := fmt.Sprintf("%s/process-groups/%s/funnels",
		baseurl(c.Config), funel.Component.ParentGroupId)
	_, err := c.JsonCall("POST", url, funel, funel)
	return err
}
func (c *Client) GetFunnel(funnelId string) (*Funnel, error) {
	url := fmt.Sprintf("%s/funnels/%s",
		baseurl(c.Config), funnelId)
	funnel := FunnelStub()
	code, err := c.JsonCall("GET", url, nil, &funnel)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	return funnel, nil
}
func (c *Client) UpdateFunnel(funnel *Funnel) error {
	url := fmt.Sprintf("%s/funnels/%s",
		baseurl(c.Config), funnel.Component.Id)
	_, err := c.JsonCall("PUT", url, funnel, funnel)
	if nil != err {
		return err
	}
	return nil
}
func (c *Client) DeleteFunnel(funnel *Funnel) error {
	url := fmt.Sprintf("%s/funnels/%s?version=%d",
		baseurl(c.Config), funnel.Component.Id, funnel.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}
