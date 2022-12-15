package nifi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type Client struct {
	Config Config
	Client *http.Client
	auth   *authentication
	// The mutex is used by the plugin to prevent parallel execution of some update/delete operations.
	// There are scenarios when updating a connection involves modifying related processors and vice versa.
	// This breaks Terraform model to some extent but at the same time is unavoidable in NiFi world.
	// Currently only flows that involve cross-resource interactions are wrapped into lock/unlock sections.
	// Most of operations can still be performed in parallel.
	Lock sync.Mutex
}

type authentication struct {
	token     string
	tlsConfig *tls.Config
}

func baseurl(conf Config) string {
	return fmt.Sprintf("%s://%s/%s", conf.HttpScheme, conf.Host, conf.ApiPath)
}

func (a *authentication) passwortAuth(conf Config) error {
	url := fmt.Sprintf("%s/access/token", baseurl(conf))
	tr := &http.Transport{}
	if conf.HttpScheme == "https" {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	client := &http.Client{Transport: tr}
	req := bytes.NewBuffer([]byte(fmt.Sprintf("username=%s&password=%s", conf.Username, conf.Password)))
	response, err := client.Post(url, "application/x-www-form-urlencoded; charset=UTF-8", req)
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		return fmt.Errorf("failed to generate the access token %d", response.StatusCode)
	}
	defer response.Body.Close()
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	a.token = string(bodyBytes)
	return nil
}

// Todo: havent test yet in the new version
func (a *authentication) certAuth(conf Config) error {
	cert, err := tls.LoadX509KeyPair(conf.AdminCertPath, conf.AdminKeyPath)
	if err != nil {
		return err
	}
	a.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	return nil
}

func NewClient(conf Config) (*Client, error) {
	httpClient := &http.Client{}
	auth := &authentication{}

	if conf.Username != "" && conf.Password != "" {
		err := auth.passwortAuth(conf)
		if err != nil {
			return nil, err
		}
	}
	if conf.AdminCertPath != "" && conf.AdminKeyPath != "" {
		err := auth.certAuth(conf)
		if err != nil {
			return nil, err
		}
	}
	tlsConfig := auth.tlsConfig
	if tlsConfig != nil {
		conf.HttpScheme = "https"
	}

	if conf.HttpScheme == "https" {
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}
		tlsConfig.InsecureSkipVerify = true
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		httpClient = &http.Client{Transport: transport}
	}

	client := &Client{
		Client: httpClient,
		Config: conf,
		auth:   auth,
	}

	return client, nil
}

// Common section

type Revision struct {
	Version int `json:"version"`
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type StatusCheckFn func(c *Client) bool

func (c *Client) WaitUtil(max_wait time.Duration, statusCheck StatusCheckFn) error {
	timeout := time.After(max_wait)

	for {
		exit := false
		select {
		case <-timeout:
			return fmt.Errorf("time out for waiting the status")
		default:
			if statusCheck(c) {
				exit = true
			}
			time.Sleep(3 * time.Second)
		}
		if exit {
			break
		}

	}
	return nil
}

func (c *Client) JsonCall(method string, url string, bodyIn interface{}, bodyOut interface{}) (int, error) {
	b, _ := json.Marshal(bodyIn)
	log.Printf("[DEBUG]: request data %s", string(b))

	var requestBody io.Reader = nil
	if bodyIn != nil {
		var buffer = new(bytes.Buffer)
		json.NewEncoder(buffer).Encode(bodyIn)
		requestBody = buffer
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*30))
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, url, requestBody)
	if err != nil {
		return 0, err
	}

	if bodyIn != nil {
		request.Header.Add("Content-Type", "application/json; charset=utf-8")
		request.Header.Add("Accept", "application/json")
	}
	if c.auth.token != "" {
		request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.auth.token))
	}

	response, err := c.Client.Do(request)
	if err != nil {
		return 0, err
	}

	log.Printf("[DEBUG]: http call to %s resulted in code: %d", url, response.StatusCode)
	defer response.Body.Close()

	if response.StatusCode >= 300 {
		bodyBytes, err := io.ReadAll(response.Body)
		if err != nil {
			return 0, err
		}
		return response.StatusCode, fmt.Errorf("the call has failed with the code of %d , the result is %s", response.StatusCode, string(bodyBytes))
	}

	if bodyOut != nil {
		err = json.NewDecoder(response.Body).Decode(bodyOut)
		if err != nil {
			return response.StatusCode, err
		}
	}

	return response.StatusCode, nil
}

//User Tennants
type Tenant struct {
	Id string `json:"id"`
}

type TenantSearchResult struct {
	Users      []Tenant `json:"users"`
	UserGroups []Tenant `json:"userGroups"`
}

type UserComponent struct {
	Id            string    `json:"id,omitempty"`
	ParentGroupId string    `json:"parentGroupId,omitempty"`
	Identity      string    `json:"identity,omitempty"`
	Position      *Position `json:"position,omitempty"`
}

func (uc UserComponent) String() string {
	return fmt.Sprintf("Id:%v ParentGroupID:%v, Identity:%v", uc.Id, uc.ParentGroupId, uc.Identity)
}

func (u User) ToTenant() *Tenant {
	return &Tenant{
		Id: u.Component.Id,
	}
}

type User struct {
	Revision  Revision      `json:"revision"`
	Component UserComponent `json:"component"`
}

func (u User) String() string {
	return fmt.Sprintf("User: {Component :{%v}}", u.Component)
}
func UserStub() *User {
	return &User{
		Component: UserComponent{
			Position: &Position{},
		},
	}
}
func (c *Client) CreateUser(user *User) error {
	url := fmt.Sprintf("%s/tenants/users",
		baseurl(c.Config))
	_, err := c.JsonCall("POST", url, user, user)
	return err
}
func (c *Client) GetUser(userId string) (*User, error) {
	url := fmt.Sprintf("%s/tenants/users/%s",
		baseurl(c.Config), userId)
	user := UserStub()
	code, err := c.JsonCall("GET", url, nil, &user)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	return user, nil
}

func (c *Client) GetUserIdsWithIdentity(userIden string) ([]string, error) {
	//https://localhost:9443/nifi-api/tenants/search-results?q=test_user

	searchResult := TenantSearchResult{}

	url := fmt.Sprintf("%s/tenants/search-results?q=%s",
		baseurl(c.Config), userIden)

	code, err := c.JsonCall("GET", url, nil, &searchResult)

	userIds := []string{}
	if code == 404 {
		return userIds, fmt.Errorf("not_found")
	}
	if nil != err {
		return userIds, err
	}
	for i := 0; i < len(searchResult.Users); i++ {
		foundId := searchResult.Users[i].Id
		userIds = append(userIds, foundId)
	}
	return userIds, nil
}

func (c *Client) DeleteUser(user *User) error {
	url := fmt.Sprintf("%s/tenants/users/%s?version=%d",
		baseurl(c.Config), user.Component.Id, user.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

//Group Tennants
type GroupComponent struct {
	Id            string    `json:"id,omitempty"`
	ParentGroupId string    `json:"parentGroupId,omitempty"`
	Identity      string    `json:"identity,omitempty"`
	Position      *Position `json:"position,omitempty"`
	Users         []Tenant  `json:"users,omitempty"`
}

func (c GroupComponent) String() string {
	return fmt.Sprintf("Id: %v ParentGroupID: %v, Identity: %v", c.Id, c.ParentGroupId, c.Identity)
}

type Group struct {
	Revision  Revision       `json:"revision"`
	Component GroupComponent `json:"component"`
}

func (c Group) String() string {
	return fmt.Sprintf("Group: { Component:{ %v } }", c.Component)
}

func GroupStub() *Group {
	return &Group{
		Component: GroupComponent{
			Position: &Position{},
		},
	}
}
func (c *Client) CreateGroup(group *Group) error {
	url := fmt.Sprintf("%s/tenants/user-groups",
		baseurl(c.Config))
	_, err := c.JsonCall("POST", url, group, group)
	return err
}
func (c *Client) GetGroup(groupId string) (*Group, error) {
	url := fmt.Sprintf("%s/tenants/user-groups/%s",
		baseurl(c.Config), groupId)
	group := GroupStub()
	code, err := c.JsonCall("GET", url, nil, &group)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	return group, nil
}
func (c *Client) GetGroupIdsWithIdentity(groupIden string) ([]string, error) {
	//https://localhost:9443/nifi-api/tenants/search-results?q=test_user

	searchResult := TenantSearchResult{}

	url := fmt.Sprintf("%s/tenants/search-results?q=%s",
		baseurl(c.Config), groupIden)

	code, err := c.JsonCall("GET", url, nil, &searchResult)

	groupIds := []string{}
	if code == 404 {
		return groupIds, fmt.Errorf("not_found")
	}
	if nil != err {
		return groupIds, err
	}
	for i := 0; i < len(searchResult.UserGroups); i++ {
		foundId := searchResult.UserGroups[i].Id
		groupIds = append(groupIds, foundId)
	}
	return groupIds, nil
}
func (c *Client) UpdateGroup(group *Group) error {
	url := fmt.Sprintf("%s/tenants/user-groups/%s",
		baseurl(c.Config), group.Component.Id)
	_, err := c.JsonCall("PUT", url, group, group)
	if nil != err {
		return err
	}
	return nil
}
func (c *Client) DeleteGroup(group *Group) error {
	url := fmt.Sprintf("%s/tenants/user-groups/%s?version=%d",
		baseurl(c.Config), group.Component.Id, group.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

//remote process group
type RemoteProcessGroupComponent struct {
	Id                string   `json:"id,omitempty"`
	ParentGroupId     string   `json:"parentGroupId"`
	Name              string   `json:"name"`
	Position          Position `json:"position"`
	TargetUris        string   `json:"targetUris"`
	TransportProtocol string   `json:"transportProtocol"`
}

type RemoteProcessGroup struct {
	Revision  Revision                    `json:"revision"`
	Component RemoteProcessGroupComponent `json:"component"`
}

func (c *Client) CreateRemoteProcessGroup(processGroup *RemoteProcessGroup) error {
	url := fmt.Sprintf("%s/process-groups/%s/remote-process-groups",
		baseurl(c.Config), processGroup.Component.ParentGroupId)
	_, err := c.JsonCall("POST", url, processGroup, processGroup)
	return err
}

func (c *Client) GetRemoteProcessGroup(processGroupId string) (*RemoteProcessGroup, error) {
	url := fmt.Sprintf("%s/remote-process-groups/%s",
		baseurl(c.Config), processGroupId)
	processGroup := RemoteProcessGroup{}
	code, err := c.JsonCall("GET", url, nil, &processGroup)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}
	return &processGroup, nil
}

func (c *Client) UpdateRemoteProcessGroup(processGroup *RemoteProcessGroup) error {
	url := fmt.Sprintf("%s/remote-process-groups/%s",
		baseurl(c.Config), processGroup.Component.Id)
	_, err := c.JsonCall("PUT", url, processGroup, processGroup)
	return err
}

func (c *Client) DeleteRemoteProcessGroup(processGroup *RemoteProcessGroup) error {
	url := fmt.Sprintf("%s/process-groups/%s?version=%d",
		baseurl(c.Config), processGroup.Component.Id, processGroup.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}

// ReportingTask section

type ReportingTaskComponent struct {
	Id                 string                 `json:"id,omitempty"`
	ParentGroupId      string                 `json:"parentGroupId,omitempty"`
	Name               string                 `json:"name,omitempty"`
	Type               string                 `json:"type,omitempty"`
	Comments           string                 `json:"comments"`
	SchedulingStrategy string                 `json:"schedulingStrategy"`
	SchedulingPeriod   string                 `json:"schedulingPeriod"`
	Properties         map[string]interface{} `json:"properties"`
}

type ReportingTask struct {
	Revision  Revision               `json:"revision"`
	Component ReportingTaskComponent `json:"component"`
}

func (c *Client) CreateReportingTask(reportingTask *ReportingTask) error {
	url := fmt.Sprintf("%s/controller/reporting-tasks",
		baseurl(c.Config))
	_, err := c.JsonCall("POST", url, reportingTask, reportingTask)
	if nil != err {
		return err
	}
	c.CleanupNilProperties(reportingTask.Component.Properties)
	return nil
}

func (c *Client) GetReportingTask(reportingTaskId string) (*ReportingTask, error) {
	url := fmt.Sprintf("%s/reporting-tasks/%s",
		baseurl(c.Config), reportingTaskId)
	reportingTask := ReportingTask{}
	code, err := c.JsonCall("GET", url, nil, &reportingTask)
	if code == 404 {
		return nil, fmt.Errorf("not_found")
	}
	if nil != err {
		return nil, err
	}

	c.CleanupNilProperties(reportingTask.Component.Properties)
	return &reportingTask, nil
}

func (c *Client) UpdateReportingTask(reportingTask *ReportingTask) error {
	url := fmt.Sprintf("%s/reporting-tasks/%s",
		baseurl(c.Config), reportingTask.Component.Id)
	_, err := c.JsonCall("PUT", url, reportingTask, reportingTask)
	if nil != err {
		return err
	}
	return nil
}

func (c *Client) DeleteReportingTask(reportingTask *ReportingTask) error {
	url := fmt.Sprintf("%s/reporting-tasks/%s?version=%d",
		baseurl(c.Config), reportingTask.Component.Id, reportingTask.Revision.Version)
	_, err := c.JsonCall("DELETE", url, nil, nil)
	return err
}
