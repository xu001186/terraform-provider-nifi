package nifi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setup() *Client {
	config := Config{
		Host:       "yanan001:8443",
		ApiPath:    "nifi-api",
		HttpScheme: "https",
		Username:   "5ac49eed-8bc1-4d61-942d-d8f555f42af0",
		Password:   "rg/Kr5ljG/0D8gNn/xr6EkGioAFlhsL1",
	}
	client, err := NewClient(config)
	if err != nil {
		panic(err)
	}
	return client
}

func TestClientReportingTaskCreate(t *testing.T) {
	config := Config{
		Host:    "127.0.0.1:8090",
		ApiPath: "nifi-api",
	}
	client, err := NewClient(config)

	processGroup := ProcessGroup{
		Revision: Revision{
			Version: 0,
		},
		Component: ProcessGroupComponent{
			ParentGroupId: "root",
			Name:          "aws_test_2",
			Position: Position{
				X: 0,
				Y: 0,
			},
		},
	}
	err = client.CreateProcessGroup(&processGroup)
	time.Sleep(5000 * time.Millisecond)
	assert.Nil(t, err)
	assert.NotEmpty(t, processGroup.Component.Id)

	reportingTask := ReportingTask{
		Revision: Revision{
			Version: 0,
		},
		Component: ReportingTaskComponent{
			ParentGroupId:      processGroup.Component.Id,
			Name:               "aws_reportingtask",
			Type:               "org.apache.nifi.controller.MonitorDiskUsage",
			Comments:           "For testing",
			SchedulingStrategy: "TIMER_DRIVEN",
			SchedulingPeriod:   "5 min",
			Properties: map[string]interface{}{
				"Threshold":          "80%",
				"Directory Location": "/",
			},
		},
	}

	err = client.CreateReportingTask(&reportingTask)
	assert.Nil(t, err)
	assert.NotEmpty(t, reportingTask.Component.Id)

	reportingTask.Component.Name = "aws_reporting_task_mod"
	err = client.UpdateReportingTask(&reportingTask)
	assert.Nil(t, err)
	assert.NotEmpty(t, reportingTask.Component.Id)

	err = client.DeleteReportingTask(&reportingTask)
	assert.Nil(t, err)

	client.DeleteProcessGroup(&processGroup)
	assert.Nil(t, err)
}
