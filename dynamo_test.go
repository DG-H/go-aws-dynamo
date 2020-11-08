package awshelper

import (
    "testing"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/magiconair/properties/assert"
)

func InitConfigForTest() {
}

type TestIDynamoDB struct {
    TableName string
}

func (t *TestIDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
    return nil, nil
}
func (t *TestIDynamoDB) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
    listTable := new(dynamodb.ListTablesOutput)
    listTable.TableNames = []*string{
        &t.TableName,
    }
    return listTable, nil
}
func (t *TestIDynamoDB) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
    return nil, nil
}
func (t *TestIDynamoDB) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
    outItems := new(dynamodb.ScanOutput)
    outItems.SetItems([]map[string]*dynamodb.AttributeValue{
        {"Test": {
            S: aws.String("NameOfTest"),
        }},
    })
    return outItems, nil
}

func (t *TestIDynamoDB) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
    outItems := new(dynamodb.QueryOutput)
    outItems.SetItems([]map[string]*dynamodb.AttributeValue{
        {"Test": {
            S: aws.String("NameOfTest"),
        }},
    })
    return outItems, nil
}

func (t *TestIDynamoDB) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
    return nil, nil
}

func TestDynamoDBTableList(t *testing.T) {

    InitConfigForTest()

    testDynamo := new(TestIDynamoDB)
    testDynamo.TableName = "TestTable"
    testRegion := ""

    result := DynamoDBTableList(testDynamo, &testRegion)
    testResult := "TestTable"

    assert.Equal(t, result, []*string{&testResult})
}

func TestDynamoDBInsertValues(t *testing.T) {

    InitConfigForTest()

    testDynamo := new(TestIDynamoDB)
    dummyTable := ""
    dummyRegion := ""
    result := DynamoDBInsertValues(testDynamo, &dummyTable, &dummyRegion, map[string]string{})

    assert.Equal(t, result, true)
}

func TestDynamoDBScan(t *testing.T) {

    InitConfigForTest()

    testDynamo := new(TestIDynamoDB)
    result := DynamoDBScan(testDynamo, nil)

    assert.Equal(t, result, []map[string]*dynamodb.AttributeValue{
        {"Test": {
            S: aws.String("NameOfTest"),
        }},
    })
}

func TestDynamoDBUpdateValues(t *testing.T) {

    InitConfigForTest()

    testDynamo := new(TestIDynamoDB)
    result := DynamoDBUpdateValues(testDynamo, nil)

    assert.Equal(t, result, true)
}

func TestDynamoDBQuery(t *testing.T) {

    InitConfigForTest()

    testDynamo := new(TestIDynamoDB)
    result := DynamoDBQuery(testDynamo, nil)

    assert.Equal(t, result, []map[string]*dynamodb.AttributeValue{
        {"Test": {
            S: aws.String("NameOfTest"),
        }},
    })
}