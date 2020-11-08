package awshelper

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

func GetSession(region *string, role *string) *session.Session {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(*region),
	}))

	if *role != "" {
		creds := assumeRole(sess, role)
		stssess, err := session.NewSession(&aws.Config{
			Credentials: creds,
			Region:      aws.String(*region),
		})

		if err != nil {
			fmt.Print("Sts error")
			return nil
		}

		sess = stssess
	}

	return sess
}

func assumeRole(sess client.ConfigProvider, role *string) *credentials.Credentials {
	creds := stscreds.NewCredentials(sess, *role)
	_, err := creds.Get()
	if err == nil {
		fmt.Println("Assume role success")
	} else {
		fmt.Print("Sts error")
		panic(err)
	}

	return creds
}
