package awshelper

import (
    "log"
    "testing"
    "time"

    "github.com/aws/aws-sdk-go/service/dynamodb"

    "github.com/magiconair/properties/assert"
)

type HealthTestIDynamoDB struct {
    TableName string
}

func (t *HealthTestIDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
    return nil, nil
}
func (t *HealthTestIDynamoDB) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
    listTable := new(dynamodb.ListTablesOutput)
    listTable.TableNames = []*string{
        &t.TableName,
    }
    return listTable, nil
}
func (t *HealthTestIDynamoDB) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
    table := new(dynamodb.DescribeTableOutput)
    return table, nil
}
func (t *HealthTestIDynamoDB) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
    return nil, nil
}
func (t *HealthTestIDynamoDB) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
    return nil, nil
}
func (t *HealthTestIDynamoDB) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
    return nil, nil
}

type TestConfig struct { 
}
func (t TestConfig) GetDynamoTable() string { return "Test" }
func (t TestConfig) GetHealthCheckTerm() int { return 5 }
func (t TestConfig) GetRegion() string { return "env" }

func TestNewDynamoDBHealthChecker(t *testing.T) {

    testDynamo := new(HealthTestIDynamoDB)

    config := TestConfig{}
    var hc DynamoDBHealthChecker
    hc = NewDynamoDBHealthChecker(testDynamo, config)

    testDynamo.TableName = "TestTable"
    time.Sleep(1000 * time.Millisecond) // Retry after go runtime is finished
    assert.Equal(t, hc.IsHealthy(), false)

    testDynamo.TableName = "sared_test"
    log.Print(testDynamo.TableName)
    time.Sleep(1000 * time.Millisecond) // Retry after go runtime is finished
    assert.Equal(t, hc.IsHealthy(), true)
}
