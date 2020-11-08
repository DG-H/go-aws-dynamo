package awshelper

import (
    "fmt"
    "log"
    "reflect"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type IDynamoDB interface {
    ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
    PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
    DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
    Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
    UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
    Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
}

func DynamoDBNewSvc(region *string) IDynamoDB {
    role := ""
    return dynamodb.New(GetSession(region, &role))
}

func DescribeTable(svc IDynamoDB, tableName string) *dynamodb.DescribeTableOutput{
    input := &dynamodb.DescribeTableInput{
        TableName: aws.String(tableName),
    }
    
    result, err := svc.DescribeTable(input)
    if err != nil {
        if aerr, ok := err.(awserr.Error); ok {
            switch aerr.Code() {
            case dynamodb.ErrCodeResourceNotFoundException:
                fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
            case dynamodb.ErrCodeInternalServerError:
                fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
            default:
                fmt.Println(aerr.Error())
            }
        } else {
            // Print the error, cast err to awserr.Error to get the Code and
            // Message from an error.
            fmt.Println(err.Error())
        }
        return nil
    }
    return result
}

func DynamoDBTableList(svc IDynamoDB, region *string) []*string {
    
    input := &dynamodb.ListTablesInput{}

    result, err := svc.ListTables(input)
    if err != nil {
        if aerr, ok := err.(awserr.Error); ok {
            switch aerr.Code() {
            case dynamodb.ErrCodeInternalServerError:
                fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
            default:
                fmt.Println(aerr.Error())
            }
        } else {
            // Print the error, cast err to awserr.Error to get the Code and
            // Message from an error.
            fmt.Println(err.Error())
        }
        return []*string {}
    }

    return result.TableNames
}


func DynamoDBInsertValues(svc IDynamoDB, tableName *string, region *string, data map[string]string) bool {
    dataMap := map[string]*dynamodb.AttributeValue{}

    for k, v := range data {
        attr, err := dynamodbattribute.Marshal(v)
        if err != nil {
            return false
        }
        dataMap[k] = attr
    }

    input := &dynamodb.PutItemInput{
        Item:                   dataMap,
        ReturnConsumedCapacity: aws.String("TOTAL"),
        TableName:              aws.String(*tableName),
    }

    result, err := svc.PutItem(input)
    if err != nil {
        if aerr, ok := err.(awserr.Error); ok {
            switch aerr.Code() {
            case dynamodb.ErrCodeConditionalCheckFailedException:
                fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
            case dynamodb.ErrCodeProvisionedThroughputExceededException:
                fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
            case dynamodb.ErrCodeResourceNotFoundException:
                fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
            case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
                fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
            case dynamodb.ErrCodeTransactionConflictException:
                fmt.Println(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
            case dynamodb.ErrCodeRequestLimitExceeded:
                fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
            case dynamodb.ErrCodeInternalServerError:
                fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
            default:
                fmt.Println(aerr.Error())
            }
        } else {
            // Print the error, cast err to awserr.Error to get the Code and
            // Message from an error.
            fmt.Println(err.Error())
        }
        return false
    }
    log.Println(result)
    return true
}

func DynamoDBUpdateValues(svc IDynamoDB, input *dynamodb.UpdateItemInput) bool {

    result, err := svc.UpdateItem(input)
    if err != nil {
        if aerr, ok := err.(awserr.Error); ok {
            switch aerr.Code() {
            case dynamodb.ErrCodeConditionalCheckFailedException:
                fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
            case dynamodb.ErrCodeProvisionedThroughputExceededException:
                fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
            case dynamodb.ErrCodeResourceNotFoundException:
                fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
            case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
                fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
            case dynamodb.ErrCodeTransactionConflictException:
                fmt.Println(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
            case dynamodb.ErrCodeRequestLimitExceeded:
                fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
            case dynamodb.ErrCodeInternalServerError:
                fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
            default:
                fmt.Println(aerr.Error())
            }
        } else {
            // Print the error, cast err to awserr.Error to get the Code and
            // Message from an error.
            fmt.Println(err.Error())
        }
        return false
    }
    log.Println(result)
    return true
}

func DynamoDBScan(svc IDynamoDB, input *dynamodb.ScanInput) []map[string]*dynamodb.AttributeValue {

    result, err := svc.Scan(input)
    if err != nil {
        if aerr, ok := err.(awserr.Error); ok {
            switch aerr.Code() {
            case dynamodb.ErrCodeProvisionedThroughputExceededException:
                fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
            case dynamodb.ErrCodeResourceNotFoundException:
                fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
            case dynamodb.ErrCodeRequestLimitExceeded:
                fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
            case dynamodb.ErrCodeInternalServerError:
                fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
            default:
                fmt.Println(aerr.Error())
            }
        } else {
            // Print the error, cast err to awserr.Error to get the Code and
            // Message from an error.
            fmt.Println(err.Error())
        }
        return nil
    }

    scanedResult := result.Items
    if _, ok := result.LastEvaluatedKey["service_name"]; ok {
        fmt.Println(reflect.TypeOf(scanedResult))
        input.ExclusiveStartKey = result.LastEvaluatedKey
        res := DynamoDBScan(svc, input)
        fmt.Println(reflect.TypeOf(res))
        for _, item := range res {
            scanedResult = append(scanedResult, item)	
        }
    }

    return scanedResult
}

func DynamoDBQuery(svc IDynamoDB, input *dynamodb.QueryInput) []map[string]*dynamodb.AttributeValue {

    result, err := svc.Query(input)
    if err != nil {
        if aerr, ok := err.(awserr.Error); ok {
            switch aerr.Code() {
            case dynamodb.ErrCodeProvisionedThroughputExceededException:
                fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
            case dynamodb.ErrCodeResourceNotFoundException:
                fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
            case dynamodb.ErrCodeRequestLimitExceeded:
                fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
            case dynamodb.ErrCodeInternalServerError:
                fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
            default:
                fmt.Println(aerr.Error())
            }
        } else {
            // Print the error, cast err to awserr.Error to get the Code and
            // Message from an error.
            fmt.Println(err.Error())
        }
    }
    queryResult := result.Items
    if _, ok := result.LastEvaluatedKey["service_name"]; ok {
        fmt.Println(reflect.TypeOf(queryResult))
        input.ExclusiveStartKey = result.LastEvaluatedKey
        res := DynamoDBQuery(svc, input)
        fmt.Println(reflect.TypeOf(res))
        for _, item := range res {
            queryResult = append(queryResult, item)
        }
    }

    return queryResult
}
