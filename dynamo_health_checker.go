package awshelper

import (
	"time"
)

// DynamoDBConfig configuration
type DynamoDBConfig interface {
	GetDynamoTable() string
	GetHealthCheckTerm() int
	GetRegion() string
}

// DynamoDBHealthChecker DB existing check tool
type DynamoDBHealthChecker struct {
	config     DynamoDBConfig
	tableExist bool
	lastCheck  time.Time
	dynamoSvc  IDynamoDB
}

// NewDynamoDBHealthChecker Get Healthchecker instance
func NewDynamoDBHealthChecker(dynamo IDynamoDB, cf DynamoDBConfig) DynamoDBHealthChecker {
	return DynamoDBHealthChecker{
		tableExist: false,
		lastCheck:  time.Time{},
		config:     cf,
		dynamoSvc:  dynamo,
	}
}

func (d *DynamoDBHealthChecker) IsHealthy() bool {
	spends := time.Since(d.lastCheck)
	if spends.Seconds() > float64(d.config.GetHealthCheckTerm()) {
		d.lastCheck = time.Now()
		go func() {
			d.tableCheck()
		}()
	}
	return d.tableExist
}

func (d *DynamoDBHealthChecker) tableCheck() {
	table := d.config.GetDynamoTable()
	searchedTable := DescribeTable(d.dynamoSvc, table)
	if searchedTable == nil {
		d.tableExist = false
		return
	}
	d.tableExist = true
}
